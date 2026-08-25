use std::collections::BTreeSet;

use anyhow::{bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use super::{mime::is_scheduling_calendar_part, SubmittedRecipientInput};
use crate::mail::{parse_calendar_meeting_request, parse_calendar_meeting_response};
use crate::{
    calendar_attendee_labels,
    mapi_events::{
        lock_calendar_event_uid_in_tx, CALENDAR_EVENT_PROJECTION_MAPI_SUBMISSION_PLACEHOLDER,
    },
    normalize_calendar_meeting_uid, normalize_email, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, AttachmentUploadInput, CalendarMeetingRequest,
    CalendarMeetingResponse, CalendarOrganizerMetadata, CalendarParticipantMetadata,
    CalendarParticipantsMetadata, Storage,
};

const LOCK_REQUEST_EVENT_CANDIDATES_SQL: &str = r#"
    SELECT id, uid, sequence, projection_state,
           starts_at = $4::timestamptz AS start_matches,
           ends_at = $5::timestamptz AS end_matches,
           organizer_json::text AS organizer_json,
           attendees_json::text AS attendees_json
    FROM calendar_events
    WHERE tenant_id = $1
      AND owner_account_id = $2
      AND uid = $3
      AND lifecycle_state = 'active'
      AND status <> 'cancelled'
      AND COALESCE(btrim(recurrence_rule), '') = ''
      AND recurrence_json = '{}'::jsonb
      AND recurrence_exceptions_json = '[]'::jsonb
      AND exception_for_event_id IS NULL
      AND exception_recurrence_id IS NULL
    ORDER BY id
    FOR UPDATE
"#;

const LOCK_REQUEST_UID_EVENTS_SQL: &str = r#"
    SELECT id
    FROM calendar_events
    WHERE tenant_id = $1
      AND owner_account_id = $2
      AND uid = $3
    ORDER BY id
    FOR UPDATE
"#;

#[derive(Debug)]
struct MeetingEventCandidate {
    uid: String,
    sequence: i32,
    projection_state: String,
    start_matches: bool,
    end_matches: bool,
    organizer_json: String,
    attendees_json: String,
}

#[derive(Debug)]
enum SelectedOutboundSchedulingBody {
    Request(CalendarMeetingRequest),
    Response(CalendarMeetingResponse),
}

pub(super) async fn validate_outbound_scheduling_body_in_tx(
    storage: &Storage,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    authorized_from: &str,
    allow_initial_mapi_event_placeholder: bool,
    subject: &str,
    body_text: &str,
    body_html: Option<&str>,
    attachments: &[AttachmentUploadInput],
    visible_recipients: &[(&'static str, SubmittedRecipientInput)],
    bcc_recipients: &[SubmittedRecipientInput],
) -> Result<()> {
    let Some(scheduling_body) = selected_outbound_scheduling_body(attachments)? else {
        return Ok(());
    };
    let authorized_from = normalize_email(authorized_from);
    let request = match scheduling_body {
        SelectedOutboundSchedulingBody::Request(request) => request,
        SelectedOutboundSchedulingBody::Response(response) => {
            return validate_outbound_response_envelope(
                &response,
                &authorized_from,
                visible_recipients,
                bcc_recipients,
            );
        }
    };
    let request_attendees =
        validate_request_envelope(&request, visible_recipients, bcc_recipients)?;

    if !request_organizer_matches(&request, &authorized_from) {
        bail!("outbound meeting request organizer does not match the authorized mailbox");
    }

    storage
        .lock_account_sync_state_in_tx(tx, tenant_id, account_id, "mail")
        .await?;
    lock_calendar_event_uid_in_tx(tx, tenant_id, account_id, &request.uid).await?;
    let initial_placeholder_allowed = initial_mapi_event_placeholder_is_allowed(
        allow_initial_mapi_event_placeholder,
        &request,
        subject,
    );
    let mut candidates =
        lock_request_event_candidates_in_tx(tx, tenant_id, account_id, &request).await?;
    if candidates.is_empty()
        && initial_placeholder_allowed
        && lock_request_uid_events_in_tx(tx, tenant_id, account_id, &request)
            .await?
            .is_empty()
    {
        create_initial_mapi_event_placeholder_in_tx(
            tx,
            tenant_id,
            account_id,
            &request,
            &authorized_from,
            subject,
            body_text,
            body_html,
        )
        .await?;
        candidates =
            lock_request_event_candidates_in_tx(tx, tenant_id, account_id, &request).await?;
    }
    if matching_candidate_count(
        &request,
        &request_attendees,
        &authorized_from,
        initial_placeholder_allowed,
        &candidates,
    ) != 1
    {
        bail!("outbound meeting request does not correlate to exactly one active canonical Event");
    }
    Ok(())
}

async fn lock_request_event_candidates_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    request: &CalendarMeetingRequest,
) -> Result<Vec<MeetingEventCandidate>> {
    sqlx::query(LOCK_REQUEST_EVENT_CANDIDATES_SQL)
        .bind(tenant_id)
        .bind(account_id)
        .bind(normalize_calendar_meeting_uid(&request.uid))
        .bind(&request.meeting_start)
        .bind(&request.meeting_end)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(|row| -> Result<MeetingEventCandidate> {
            Ok(MeetingEventCandidate {
                uid: row.try_get("uid")?,
                sequence: row.try_get("sequence")?,
                projection_state: row.try_get("projection_state")?,
                start_matches: row.try_get("start_matches")?,
                end_matches: row.try_get("end_matches")?,
                organizer_json: row.try_get("organizer_json")?,
                attendees_json: row.try_get("attendees_json")?,
            })
        })
        .collect()
}

async fn lock_request_uid_events_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    request: &CalendarMeetingRequest,
) -> Result<Vec<Uuid>> {
    Ok(sqlx::query_scalar(LOCK_REQUEST_UID_EVENTS_SQL)
        .bind(tenant_id)
        .bind(account_id)
        .bind(normalize_calendar_meeting_uid(&request.uid))
        .fetch_all(&mut **tx)
        .await?)
}

fn initial_mapi_event_placeholder_is_allowed(
    allow_initial_mapi_event_placeholder: bool,
    request: &CalendarMeetingRequest,
    subject: &str,
) -> bool {
    allow_initial_mapi_event_placeholder
        && request.meeting_sequence == 0
        && !subject.trim().is_empty()
}

#[allow(clippy::too_many_arguments)]
async fn create_initial_mapi_event_placeholder_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    request: &CalendarMeetingRequest,
    authorized_from: &str,
    subject: &str,
    body_text: &str,
    body_html: Option<&str>,
) -> Result<()> {
    let calendar_id = Storage::ensure_default_calendar_in_tx(tx, tenant_id, account_id).await?;
    let event_id = Uuid::new_v4();
    let uid = normalize_calendar_meeting_uid(&request.uid);
    let organizer_name = request
        .organizer
        .as_ref()
        .map(|organizer| organizer.display_name.trim())
        .unwrap_or_default();
    let participants = CalendarParticipantsMetadata {
        organizer: Some(CalendarOrganizerMetadata {
            email: authorized_from.to_string(),
            common_name: organizer_name.to_string(),
        }),
        attendees: request
            .attendees
            .iter()
            .map(|attendee| CalendarParticipantMetadata {
                email: attendee.email.clone(),
                common_name: attendee.display_name.clone(),
                role: attendee.role.clone(),
                partstat: attendee.partstat.clone(),
                rsvp: attendee.rsvp,
                proposed_start: None,
                proposed_end: None,
                counter_proposal: false,
            })
            .collect(),
    };
    let organizer_json = serde_json::json!({
        "email": authorized_from,
        "common_name": organizer_name,
        "is_meeting": true,
    })
    .to_string();
    let attendees_json = serialize_calendar_participants_metadata(&participants);
    let source_payload_json = serde_json::json!({
        "attendees": calendar_attendee_labels(&participants),
        "outboundMeetingRequestPlaceholder": true,
    })
    .to_string();
    let status = if request.intended_busy_status == 1 {
        "tentative"
    } else {
        "confirmed"
    };
    let _inserted_event_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO calendar_events (
            id, tenant_id, owner_account_id, calendar_id, uid,
            starts_at, ends_at, time_zone, all_day, status, sequence,
            title, location, organizer_json, attendees_json, body_text, body_html,
            import_source, source_payload_json, projection_state
        )
        SELECT
            $1, $2, $3, $4, $5,
            $6::timestamptz, $7::timestamptz, 'UTC', FALSE, $8, $9,
            $10, $11, $12::jsonb, $13::jsonb, $14, NULLIF($15::text, ''),
            'mapi', $16::jsonb, $17
        WHERE NOT EXISTS (
            SELECT 1
            FROM calendar_events
            WHERE tenant_id = $2
              AND owner_account_id = $3
              AND uid = $5
        )
        ON CONFLICT (tenant_id, owner_account_id, calendar_id, uid) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(event_id)
    .bind(tenant_id)
    .bind(account_id)
    .bind(calendar_id)
    .bind(&uid)
    .bind(&request.meeting_start)
    .bind(&request.meeting_end)
    .bind(status)
    .bind(request.meeting_sequence)
    .bind(subject.trim())
    .bind(request.meeting_location.as_deref().unwrap_or_default())
    .bind(&organizer_json)
    .bind(&attendees_json)
    .bind(body_text)
    .bind(body_html)
    .bind(&source_payload_json)
    .bind(CALENDAR_EVENT_PROJECTION_MAPI_SUBMISSION_PLACEHOLDER)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(())
}

fn selected_outbound_scheduling_body(
    attachments: &[AttachmentUploadInput],
) -> Result<Option<SelectedOutboundSchedulingBody>> {
    let mut selected_parts = attachments
        .iter()
        .filter(|attachment| is_scheduling_calendar_part(attachment));
    let Some(selected) = selected_parts.next() else {
        return Ok(None);
    };
    if selected_parts.next().is_some() {
        bail!("outbound message has more than one selected scheduling body");
    }
    if let Some(request) = parse_calendar_meeting_request(std::slice::from_ref(selected)) {
        return Ok(Some(SelectedOutboundSchedulingBody::Request(request)));
    }
    if let Some(response) = parse_calendar_meeting_response(std::slice::from_ref(selected)) {
        return Ok(Some(SelectedOutboundSchedulingBody::Response(response)));
    }
    bail!("outbound scheduling body is invalid or uses an unsupported calendar method")
}

fn unique_request_attendees(request: &CalendarMeetingRequest) -> Result<BTreeSet<String>> {
    let mut attendees = BTreeSet::new();
    for attendee in &request.attendees {
        let email = normalize_email(&attendee.email);
        if email.is_empty() || !attendees.insert(email) {
            bail!("outbound meeting request attendees must be nonempty and unique");
        }
    }
    if attendees.is_empty() {
        bail!("outbound meeting request requires at least one attendee");
    }
    Ok(attendees)
}

fn request_organizer_matches(request: &CalendarMeetingRequest, authorized_from: &str) -> bool {
    let authorized_from = normalize_email(authorized_from);
    !authorized_from.is_empty()
        && request
            .organizer
            .as_ref()
            .map(|organizer| normalize_email(&organizer.email))
            .as_deref()
            == Some(authorized_from.as_str())
}

fn unique_envelope_attendees(
    visible_recipients: &[(&'static str, SubmittedRecipientInput)],
) -> Result<BTreeSet<String>> {
    let mut attendees = BTreeSet::new();
    for (_, recipient) in visible_recipients {
        let email = normalize_email(&recipient.address);
        if email.is_empty() || !attendees.insert(email) {
            bail!("outbound meeting request visible recipients must be nonempty and unique");
        }
    }
    Ok(attendees)
}

fn validate_request_envelope(
    request: &CalendarMeetingRequest,
    visible_recipients: &[(&'static str, SubmittedRecipientInput)],
    bcc_recipients: &[SubmittedRecipientInput],
) -> Result<BTreeSet<String>> {
    if !bcc_recipients.is_empty() {
        bail!("outbound meeting requests do not support Bcc recipients");
    }
    let request_attendees = unique_request_attendees(request)?;
    if request_attendees != unique_envelope_attendees(visible_recipients)? {
        bail!("outbound meeting request attendees do not match visible envelope recipients");
    }
    Ok(request_attendees)
}

pub(super) fn validate_outbound_response_envelope(
    response: &CalendarMeetingResponse,
    authorized_from: &str,
    visible_recipients: &[(&'static str, SubmittedRecipientInput)],
    bcc_recipients: &[SubmittedRecipientInput],
) -> Result<()> {
    if !bcc_recipients.is_empty() {
        bail!("outbound meeting responses do not support Bcc recipients");
    }
    let authorized_from = normalize_email(authorized_from);
    if authorized_from.is_empty() || normalize_email(&response.attendee_email) != authorized_from {
        bail!("outbound meeting response attendee does not match the authorized mailbox");
    }
    let [(_, organizer_recipient)] = visible_recipients else {
        bail!("outbound meeting response requires exactly one visible organizer recipient");
    };
    let organizer_recipient = normalize_email(&organizer_recipient.address);
    if organizer_recipient.is_empty() {
        bail!("outbound meeting response organizer recipient is empty");
    }
    if response
        .organizer
        .as_ref()
        .is_some_and(|organizer| normalize_email(&organizer.email) != organizer_recipient)
    {
        bail!("outbound meeting response organizer does not match its visible recipient");
    }
    Ok(())
}

fn matching_candidate_count(
    request: &CalendarMeetingRequest,
    request_attendees: &BTreeSet<String>,
    authorized_from: &str,
    initial_placeholder_allowed: bool,
    candidates: &[MeetingEventCandidate],
) -> usize {
    candidates
        .iter()
        .filter(|candidate| {
            let participants = parse_calendar_participants_metadata(&candidate.attendees_json);
            candidate.start_matches
                && candidate.end_matches
                && normalize_calendar_meeting_uid(&candidate.uid)
                    == normalize_calendar_meeting_uid(&request.uid)
                && candidate_is_eligible_for_request(
                    candidate,
                    request.meeting_sequence,
                    initial_placeholder_allowed,
                )
                && canonical_organizer_matches(
                    &participants,
                    &candidate.organizer_json,
                    authorized_from,
                )
                && candidate_attendees_match(candidate, &participants, request_attendees)
        })
        .count()
}

fn candidate_attendees_match(
    candidate: &MeetingEventCandidate,
    participants: &crate::CalendarParticipantsMetadata,
    request_attendees: &BTreeSet<String>,
) -> bool {
    let Some(canonical_attendees) = unique_canonical_attendees(participants) else {
        return false;
    };
    match candidate.projection_state.as_str() {
        "visible" => request_attendees.is_subset(&canonical_attendees),
        CALENDAR_EVENT_PROJECTION_MAPI_SUBMISSION_PLACEHOLDER => {
            canonical_attendees == *request_attendees
        }
        _ => false,
    }
}

fn candidate_is_eligible_for_request(
    candidate: &MeetingEventCandidate,
    request_sequence: i32,
    initial_placeholder_allowed: bool,
) -> bool {
    match candidate.projection_state.as_str() {
        "visible" => request_sequence >= candidate.sequence,
        CALENDAR_EVENT_PROJECTION_MAPI_SUBMISSION_PLACEHOLDER => {
            initial_placeholder_allowed && request_sequence == 0 && candidate.sequence == 0
        }
        _ => false,
    }
}

fn unique_canonical_attendees(
    participants: &crate::CalendarParticipantsMetadata,
) -> Option<BTreeSet<String>> {
    let mut attendees = BTreeSet::new();
    for attendee in &participants.attendees {
        let email = normalize_email(&attendee.email);
        if email.is_empty() || !attendees.insert(email) {
            return None;
        }
    }
    (!attendees.is_empty()).then_some(attendees)
}

fn canonical_organizer_matches(
    participants: &crate::CalendarParticipantsMetadata,
    organizer_json: &str,
    authorized_from: &str,
) -> bool {
    let authorized_from = normalize_email(authorized_from);
    let mut organizer_emails = Vec::new();
    if let Some(organizer) = participants.organizer.as_ref() {
        let email = normalize_calendar_organizer_email(&organizer.email);
        if email.is_empty() {
            return false;
        }
        organizer_emails.push(email);
    }

    let Ok(value) = serde_json::from_str::<serde_json::Value>(organizer_json) else {
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    if let Some(value) = object.get("email") {
        let Some(value) = value.as_str() else {
            return false;
        };
        let email = normalize_calendar_organizer_email(value);
        if email.is_empty() {
            return false;
        }
        organizer_emails.push(email);
    }
    if let Some(value) = object.get("sendTo") {
        let Some(value) = value
            .as_object()
            .and_then(|send_to| send_to.get("imip"))
            .and_then(serde_json::Value::as_str)
        else {
            return false;
        };
        let email = normalize_calendar_organizer_email(value);
        if email.is_empty() {
            return false;
        }
        organizer_emails.push(email);
    }
    !organizer_emails.is_empty()
        && organizer_emails
            .iter()
            .all(|organizer| organizer == &authorized_from)
}

fn normalize_calendar_organizer_email(value: &str) -> String {
    let value = value.trim();
    let value = value
        .get(.."mailto:".len())
        .filter(|prefix| prefix.eq_ignore_ascii_case("mailto:"))
        .and_then(|_| value.get("mailto:".len()..))
        .unwrap_or(value);
    normalize_email(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CalendarMeetingAttendee, CalendarMeetingIdentity};

    fn request(sequence: i32) -> CalendarMeetingRequest {
        CalendarMeetingRequest {
            uid: "request-correlation@example.test".to_string(),
            transport_attachment_id: None,
            client_processed: false,
            organizer: Some(CalendarMeetingIdentity {
                email: "organizer@example.test".to_string(),
                display_name: "Organizer".to_string(),
            }),
            attendees: vec![CalendarMeetingAttendee {
                email: "first@example.test".to_string(),
                display_name: "First".to_string(),
                cutype: "INDIVIDUAL".to_string(),
                role: "REQ-PARTICIPANT".to_string(),
                partstat: "needs-action".to_string(),
                rsvp: true,
            }],
            response_requested: true,
            sent_at: Some("2026-08-24T08:00:00Z".to_string()),
            meeting_start: "2026-08-25T09:00:00Z".to_string(),
            meeting_end: "2026-08-25T10:00:00Z".to_string(),
            meeting_location: None,
            meeting_sequence: sequence,
            intended_busy_status: 2,
        }
    }

    fn candidate(sequence: i32) -> MeetingEventCandidate {
        MeetingEventCandidate {
            uid: "request-correlation@example.test".to_string(),
            sequence,
            projection_state: "visible".to_string(),
            start_matches: true,
            end_matches: true,
            organizer_json:
                r#"{"email":"organizer@example.test","is_meeting":true}"#.to_string(),
            attendees_json: r#"{
                "organizer":{"email":"organizer@example.test","common_name":"Organizer"},
                "attendees":[
                    {"email":"first@example.test","common_name":"First","role":"REQ-PARTICIPANT","partstat":"needs-action","rsvp":true}
                ]
            }"#
            .to_string(),
        }
    }

    fn attendees(request: &CalendarMeetingRequest) -> BTreeSet<String> {
        unique_request_attendees(request).unwrap()
    }

    fn scheduling_attachment(method: &str, body: &str) -> AttachmentUploadInput {
        AttachmentUploadInput {
            file_name: "invite.ics".to_string(),
            media_type: format!("text/calendar; method={method}; charset=UTF-8"),
            disposition: Some("inline".to_string()),
            content_id: None,
            is_scheduling_body: true,
            blob_bytes: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn canonical_request_matches_one_visible_event_with_partial_attendees() {
        let request = request(3);
        let mut event = candidate(3);
        event.attendees_json = event.attendees_json.replace(
            "]",
            r#",{"email":"second@example.test","common_name":"Second","role":"REQ-PARTICIPANT","partstat":"needs-action","rsvp":true}]"#,
        );
        assert_eq!(
            matching_candidate_count(
                &request,
                &attendees(&request),
                "organizer@example.test",
                false,
                &[event],
            ),
            1
        );
    }

    #[test]
    fn missing_and_ambiguous_events_are_not_exact_correlations() {
        let request = request(3);
        assert_eq!(
            matching_candidate_count(
                &request,
                &attendees(&request),
                "organizer@example.test",
                false,
                &[],
            ),
            0
        );
        assert_eq!(
            matching_candidate_count(
                &request,
                &attendees(&request),
                "organizer@example.test",
                false,
                &[candidate(3), candidate(3)],
            ),
            2
        );
    }

    #[test]
    fn only_an_initial_direct_mapi_request_can_precede_its_event() {
        let initial = request(0);
        assert!(initial_mapi_event_placeholder_is_allowed(
            true, &initial, "Probe"
        ));
        assert!(!initial_mapi_event_placeholder_is_allowed(
            false, &initial, "Probe"
        ));
        assert!(!initial_mapi_event_placeholder_is_allowed(
            true,
            &request(1),
            "Probe"
        ));
        assert!(!initial_mapi_event_placeholder_is_allowed(
            true, &initial, " "
        ));
    }

    #[test]
    fn hidden_placeholder_only_correlates_to_the_initial_direct_mapi_request() {
        let initial = request(0);
        let mut placeholder = candidate(0);
        placeholder.projection_state =
            CALENDAR_EVENT_PROJECTION_MAPI_SUBMISSION_PLACEHOLDER.to_string();
        assert_eq!(
            matching_candidate_count(
                &initial,
                &attendees(&initial),
                "organizer@example.test",
                true,
                std::slice::from_ref(&placeholder),
            ),
            1
        );
        assert_eq!(
            matching_candidate_count(
                &initial,
                &attendees(&initial),
                "organizer@example.test",
                false,
                std::slice::from_ref(&placeholder),
            ),
            0
        );
        let update = request(1);
        assert_eq!(
            matching_candidate_count(
                &update,
                &attendees(&update),
                "organizer@example.test",
                true,
                &[placeholder],
            ),
            0
        );

        let mut extra_attendee = candidate(0);
        extra_attendee.projection_state =
            CALENDAR_EVENT_PROJECTION_MAPI_SUBMISSION_PLACEHOLDER.to_string();
        extra_attendee.attendees_json = extra_attendee.attendees_json.replace(
            "]",
            r#",{"email":"extra@example.test","common_name":"Extra","role":"REQ-PARTICIPANT","partstat":"needs-action","rsvp":true}]"#,
        );
        assert_eq!(
            matching_candidate_count(
                &initial,
                &attendees(&initial),
                "organizer@example.test",
                true,
                &[extra_attendee],
            ),
            0
        );
    }

    #[test]
    fn request_uid_interval_organizer_and_attendee_mismatches_fail() {
        let request = request(3);
        assert!(!request_organizer_matches(
            &request,
            "different-organizer@example.test"
        ));
        let mut mismatches = Vec::new();
        let mut uid = candidate(3);
        uid.uid = "other@example.test".to_string();
        mismatches.push(uid);
        let mut start = candidate(3);
        start.start_matches = false;
        mismatches.push(start);
        let mut end = candidate(3);
        end.end_matches = false;
        mismatches.push(end);
        let mut organizer = candidate(3);
        organizer.organizer_json =
            r#"{"email":"other@example.test","is_meeting":true}"#.to_string();
        mismatches.push(organizer);
        let mut attendee = candidate(3);
        attendee.attendees_json = attendee
            .attendees_json
            .replace("first@example.test", "other@example.test");
        mismatches.push(attendee);
        for mismatch in mismatches {
            assert_eq!(
                matching_candidate_count(
                    &request,
                    &attendees(&request),
                    "organizer@example.test",
                    false,
                    &[mismatch],
                ),
                0
            );
        }
    }

    #[test]
    fn lower_sequence_is_stale_while_equal_and_higher_sequences_match() {
        for (request_sequence, expected) in [(2, 0), (3, 1), (4, 1)] {
            let request = request(request_sequence);
            assert_eq!(
                matching_candidate_count(
                    &request,
                    &attendees(&request),
                    "organizer@example.test",
                    false,
                    &[candidate(3)],
                ),
                expected
            );
        }
    }

    #[test]
    fn duplicate_or_envelope_inconsistent_request_attendees_fail() {
        let mut duplicate_request = request(3);
        duplicate_request
            .attendees
            .push(duplicate_request.attendees[0].clone());
        assert!(unique_request_attendees(&duplicate_request).is_err());

        let recipients = vec![
            (
                "to",
                SubmittedRecipientInput {
                    address: "first@example.test".to_string(),
                    display_name: None,
                },
            ),
            (
                "cc",
                SubmittedRecipientInput {
                    address: "first@example.test".to_string(),
                    display_name: None,
                },
            ),
        ];
        assert!(unique_envelope_attendees(&recipients).is_err());

        let request = request(3);
        let visible = vec![(
            "to",
            SubmittedRecipientInput {
                address: "first@example.test".to_string(),
                display_name: None,
            },
        )];
        assert!(validate_request_envelope(
            &request,
            &visible,
            &[SubmittedRecipientInput {
                address: "hidden@example.test".to_string(),
                display_name: None,
            }],
        )
        .is_err());

        let wrong_visible = vec![(
            "to",
            SubmittedRecipientInput {
                address: "other@example.test".to_string(),
                display_name: None,
            },
        )];
        assert!(validate_request_envelope(&request, &wrong_visible, &[]).is_err());
    }

    #[test]
    fn valid_reply_uses_the_minimum_authorized_envelope_without_event_correlation() {
        let reply = scheduling_attachment(
            "REPLY",
            concat!(
                "BEGIN:VCALENDAR\r\n",
                "VERSION:2.0\r\n",
                "METHOD:REPLY\r\n",
                "BEGIN:VEVENT\r\n",
                "UID:reply\\,correlation\\;slot\\\\team@example.test\r\n",
                "ATTENDEE;PARTSTAT=ACCEPTED:mailto:first@example.test\r\n",
                "ORGANIZER:mailto:organizer@example.test\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n",
            ),
        );
        let Some(SelectedOutboundSchedulingBody::Response(response)) =
            selected_outbound_scheduling_body(&[reply]).unwrap()
        else {
            panic!("valid REPLY must remain a response scheduling body");
        };
        assert_eq!(response.uid, r"reply,correlation;slot\team@example.test");
        let organizer_recipient = vec![(
            "to",
            SubmittedRecipientInput {
                address: "organizer@example.test".to_string(),
                display_name: None,
            },
        )];
        assert!(validate_outbound_response_envelope(
            &response,
            "first@example.test",
            &organizer_recipient,
            &[],
        )
        .is_ok());
        assert!(validate_outbound_response_envelope(
            &response,
            "other-attendee@example.test",
            &organizer_recipient,
            &[],
        )
        .is_err());
        assert!(validate_outbound_response_envelope(
            &response,
            "first@example.test",
            &organizer_recipient,
            &[SubmittedRecipientInput {
                address: "hidden@example.test".to_string(),
                display_name: None,
            }],
        )
        .is_err());
        assert!(
            validate_outbound_response_envelope(&response, "first@example.test", &[], &[]).is_err()
        );

        let two_visible_recipients = vec![
            organizer_recipient[0].clone(),
            (
                "cc",
                SubmittedRecipientInput {
                    address: "other-organizer@example.test".to_string(),
                    display_name: None,
                },
            ),
        ];
        assert!(validate_outbound_response_envelope(
            &response,
            "first@example.test",
            &two_visible_recipients,
            &[],
        )
        .is_err());

        let wrong_organizer_recipient = vec![(
            "to",
            SubmittedRecipientInput {
                address: "other-organizer@example.test".to_string(),
                display_name: None,
            },
        )];
        assert!(validate_outbound_response_envelope(
            &response,
            "first@example.test",
            &wrong_organizer_recipient,
            &[],
        )
        .is_err());
    }

    #[test]
    fn malformed_or_unsupported_selected_scheduling_body_fails() {
        let malformed_request = scheduling_attachment(
            "REQUEST",
            "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nEND:VCALENDAR\r\n",
        );
        assert!(selected_outbound_scheduling_body(&[malformed_request]).is_err());

        let malformed_reply = scheduling_attachment(
            "REPLY",
            "BEGIN:VCALENDAR\r\nMETHOD:REPLY\r\nEND:VCALENDAR\r\n",
        );
        assert!(selected_outbound_scheduling_body(&[malformed_reply]).is_err());

        let unsupported_cancel = scheduling_attachment(
            "CANCEL",
            concat!(
                "BEGIN:VCALENDAR\r\n",
                "METHOD:CANCEL\r\n",
                "BEGIN:VEVENT\r\n",
                "UID:cancel-correlation@example.test\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n",
            ),
        );
        assert!(selected_outbound_scheduling_body(&[unsupported_cancel]).is_err());
    }

    #[test]
    fn candidate_lock_query_excludes_cancelled_deleted_and_recurring_events() {
        for required in [
            "lifecycle_state = 'active'",
            "status <> 'cancelled'",
            "COALESCE(btrim(recurrence_rule), '') = ''",
            "recurrence_json = '{}'::jsonb",
            "recurrence_exceptions_json = '[]'::jsonb",
            "exception_for_event_id IS NULL",
            "exception_recurrence_id IS NULL",
            "ORDER BY id",
            "FOR UPDATE",
        ] {
            assert!(LOCK_REQUEST_EVENT_CANDIDATES_SQL.contains(required));
        }

        for required in [
            "FROM calendar_events",
            "owner_account_id = $2",
            "uid = $3",
            "ORDER BY id",
            "FOR UPDATE",
        ] {
            assert!(LOCK_REQUEST_UID_EVENTS_SQL.contains(required));
        }
    }

    #[test]
    fn request_correlation_locks_mail_state_before_event_candidates() {
        let source = include_str!("meeting_request.rs");
        let function = source
            .split("pub(super) async fn validate_outbound_scheduling_body_in_tx")
            .nth(1)
            .expect("outbound scheduling validator exists")
            .split("fn selected_outbound_scheduling_body")
            .next()
            .expect("outbound scheduling validator has a bounded body");
        let mail_lock = function
            .find("lock_account_sync_state_in_tx")
            .expect("request validation locks mail account state");
        let event_lock = function
            .find("sqlx::query(LOCK_REQUEST_EVENT_CANDIDATES_SQL)")
            .expect("request validation locks Event candidates");

        assert!(mail_lock < event_lock);
    }
}
