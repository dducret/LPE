use std::collections::BTreeSet;

use anyhow::{bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use super::{mime::is_scheduling_calendar_part, SubmittedRecipientInput};
use crate::mail::{parse_calendar_meeting_request, parse_calendar_meeting_response};
use crate::{
    normalize_calendar_meeting_uid, normalize_email, parse_calendar_participants_metadata,
    AttachmentUploadInput, CalendarMeetingRequest, CalendarMeetingResponse, Storage,
};

const LOCK_REQUEST_EVENT_CANDIDATES_SQL: &str = r#"
    SELECT id, uid, sequence,
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

#[derive(Debug)]
struct MeetingEventCandidate {
    uid: String,
    sequence: i32,
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
    let rows = sqlx::query(LOCK_REQUEST_EVENT_CANDIDATES_SQL)
        .bind(tenant_id)
        .bind(account_id)
        .bind(normalize_calendar_meeting_uid(&request.uid))
        .bind(&request.meeting_start)
        .bind(&request.meeting_end)
        .fetch_all(&mut **tx)
        .await?;
    let candidates = rows
        .into_iter()
        .map(|row| -> Result<MeetingEventCandidate> {
            Ok(MeetingEventCandidate {
                uid: row.try_get("uid")?,
                sequence: row.try_get("sequence")?,
                start_matches: row.try_get("start_matches")?,
                end_matches: row.try_get("end_matches")?,
                organizer_json: row.try_get("organizer_json")?,
                attendees_json: row.try_get("attendees_json")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if matching_candidate_count(&request, &request_attendees, &authorized_from, &candidates) != 1 {
        bail!("outbound meeting request does not correlate to exactly one active canonical Event");
    }
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
                && request.meeting_sequence >= candidate.sequence
                && canonical_organizer_matches(
                    &participants,
                    &candidate.organizer_json,
                    authorized_from,
                )
                && request_attendees.iter().all(|request_attendee| {
                    participants
                        .attendees
                        .iter()
                        .filter(|attendee| normalize_email(&attendee.email) == *request_attendee)
                        .count()
                        == 1
                })
        })
        .count()
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
            start_matches: true,
            end_matches: true,
            organizer_json:
                r#"{"email":"organizer@example.test","is_meeting":true}"#.to_string(),
            attendees_json: r#"{
                "organizer":{"email":"organizer@example.test","common_name":"Organizer"},
                "attendees":[
                    {"email":"first@example.test","common_name":"First","role":"REQ-PARTICIPANT","partstat":"needs-action","rsvp":true},
                    {"email":"second@example.test","common_name":"Second","role":"REQ-PARTICIPANT","partstat":"needs-action","rsvp":true}
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
    fn canonical_request_matches_one_event_and_allows_partial_attendees() {
        let request = request(3);
        assert_eq!(
            matching_candidate_count(
                &request,
                &attendees(&request),
                "organizer@example.test",
                &[candidate(3)],
            ),
            1
        );
    }

    #[test]
    fn request_before_event_save_and_ambiguous_events_fail_correlation() {
        let request = request(3);
        assert_eq!(
            matching_candidate_count(
                &request,
                &attendees(&request),
                "organizer@example.test",
                &[],
            ),
            0
        );
        assert_eq!(
            matching_candidate_count(
                &request,
                &attendees(&request),
                "organizer@example.test",
                &[candidate(3), candidate(3)],
            ),
            2
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
