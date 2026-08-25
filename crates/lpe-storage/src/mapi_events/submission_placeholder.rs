use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Result};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    calendar_attendee_labels, normalize_calendar_meeting_uid, normalize_email,
    parse_calendar_participants_metadata, serialize_calendar_participants_metadata,
    CalendarParticipantsMetadata,
};

use super::MapiEventCreateInput;

pub(crate) const CALENDAR_EVENT_PROJECTION_VISIBLE: &str = "visible";
pub(crate) const CALENDAR_EVENT_PROJECTION_MAPI_SUBMISSION_PLACEHOLDER: &str =
    "mapi_submission_placeholder";

pub(crate) async fn lock_calendar_event_uid_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    uid: &str,
) -> Result<()> {
    let (class_id, object_id) = calendar_event_uid_advisory_lock_keys(
        tenant_id,
        owner_account_id,
        &normalize_calendar_meeting_uid(uid),
    );
    sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(class_id)
        .bind(object_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(super) async fn insert_mapi_event_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    calendar_id: Uuid,
    event_id: Uuid,
    event_uid: &str,
    input: &MapiEventCreateInput,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO calendar_events (
            id, tenant_id, owner_account_id, calendar_id, uid,
            starts_at, ends_at, time_zone, all_day, status, sequence,
            recurrence_rule, recurrence_json, recurrence_exceptions_json,
            title, location, organizer_json, attendees_json, body_text, body_html,
            import_source, source_payload_json
        )
        VALUES (
            $1, $2, $3, $4, $5,
            (($6::date + $7::time) AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC')),
            ((($6::date + $7::time) AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC'))
                + make_interval(mins => GREATEST($9, 0))),
            $8,
            $10,
            COALESCE(NULLIF($11, ''), 'confirmed'),
            GREATEST($12, 0),
            NULLIF($13, ''),
            CASE WHEN NULLIF($14, '') IS NULL THEN '{}'::jsonb ELSE $14::jsonb END,
            CASE WHEN NULLIF($15, '') IS NULL THEN '[]'::jsonb ELSE $15::jsonb END,
            $16,
            $17,
            CASE WHEN NULLIF($18, '') IS NULL THEN '{}'::jsonb ELSE $18::jsonb END,
            CASE
                WHEN NULLIF($20, '') IS NOT NULL THEN $20::jsonb
                WHEN NULLIF($19, '') IS NOT NULL THEN
                    jsonb_build_object(
                        'attendees',
                        jsonb_build_array(jsonb_build_object('email', $19::text))
                    )
                ELSE '{}'::jsonb
            END,
            $21,
            NULLIF($22, ''),
            'mapi',
            jsonb_build_object('attendees', $19::text)
        )
        "#,
    )
    .bind(event_id)
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(calendar_id)
    .bind(event_uid)
    .bind(input.event.date.trim())
    .bind(input.event.time.trim())
    .bind(input.event.time_zone.trim())
    .bind(input.event.duration_minutes.max(0))
    .bind(input.event.all_day)
    .bind(input.event.status.trim())
    .bind(input.event.sequence)
    .bind(input.event.recurrence_rule.trim())
    .bind(input.event.recurrence_json.trim())
    .bind(input.event.recurrence_exceptions_json.trim())
    .bind(input.event.title.trim())
    .bind(input.event.location.trim())
    .bind(input.event.organizer_json.trim())
    .bind(input.event.attendees.trim())
    .bind(input.event.attendees_json.trim())
    .bind(input.event.notes.trim())
    .bind(input.event.body_html.trim())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn try_adopt_mapi_submission_placeholder_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    calendar_id: Uuid,
    event_uid: &str,
    input: &MapiEventCreateInput,
) -> Result<Option<Uuid>> {
    let rows = sqlx::query(
        r#"
        SELECT
            event.id,
            event.lifecycle_state,
            event.projection_state,
            event.status,
            event.sequence,
            event.organizer_json::text AS organizer_json,
            event.attendees_json::text AS attendees_json,
            event.meeting_response_state_json::text AS meeting_response_state_json,
            event.starts_at =
                (($4::date + $5::time) AT TIME ZONE COALESCE(NULLIF($6, ''), 'UTC'))
                AS start_matches,
            event.ends_at =
                ((($4::date + $5::time) AT TIME ZONE COALESCE(NULLIF($6, ''), 'UTC'))
                    + make_interval(mins => GREATEST($7, 0)))
                AS end_matches,
            COALESCE(btrim(event.recurrence_rule), '') = ''
                AND event.recurrence_json = '{}'::jsonb
                AND event.recurrence_exceptions_json = '[]'::jsonb
                AND event.exception_for_event_id IS NULL
                AND event.exception_recurrence_id IS NULL
                AS non_recurring,
            EXISTS (
                SELECT 1
                FROM mapi_object_identities identity
                WHERE identity.tenant_id = event.tenant_id
                  AND identity.canonical_id = event.id
                  AND identity.object_kind IN ('calendar_event', 'deleted_calendar_event')
            ) AS has_identity,
            EXISTS (
                SELECT 1
                FROM mapi_custom_property_values property_value
                WHERE property_value.tenant_id = event.tenant_id
                  AND property_value.account_id = event.owner_account_id
                  AND property_value.object_kind = 'calendar_event'
                  AND property_value.canonical_id = event.id
            ) AS has_custom_properties,
            EXISTS (
                SELECT 1
                FROM calendar_event_attachments attachment
                WHERE attachment.tenant_id = event.tenant_id
                  AND attachment.owner_account_id = event.owner_account_id
                  AND attachment.event_id = event.id
            ) AS has_attachments
        FROM calendar_events event
        WHERE event.tenant_id = $1
          AND event.owner_account_id = $2
          AND event.uid = $3
        ORDER BY event.id
        FOR UPDATE OF event
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(event_uid)
    .bind(input.event.date.trim())
    .bind(input.event.time.trim())
    .bind(input.event.time_zone.trim())
    .bind(input.event.duration_minutes.max(0))
    .fetch_all(&mut **tx)
    .await?;

    let placeholder_count = rows
        .iter()
        .filter(|row| {
            row.get::<String, _>("projection_state")
                == CALENDAR_EVENT_PROJECTION_MAPI_SUBMISSION_PLACEHOLDER
        })
        .count();
    if placeholder_count == 0 {
        return Ok(None);
    }
    if placeholder_count != 1 || rows.len() != 1 {
        bail!("MAPI meeting upload has an ambiguous owner-wide placeholder correlation");
    }
    let row = &rows[0];
    let event_id = row.get::<Uuid, _>("id");
    if input.imported_identity.is_none()
        || input.principal_account_id != owner_account_id
        || input.event.account_id != owner_account_id
        || input.event.id.is_some_and(|id| id != event_id)
    {
        bail!("MAPI meeting placeholder adoption requires its owner's imported Event");
    }
    if row.get::<String, _>("lifecycle_state") != "active"
        || row.get::<String, _>("status") == "cancelled"
        || input.event.status.trim().eq_ignore_ascii_case("cancelled")
        || row.get::<i32, _>("sequence") != input.event.sequence
        || !row.get::<bool, _>("start_matches")
        || !row.get::<bool, _>("end_matches")
        || !row.get::<bool, _>("non_recurring")
        || !input_is_non_recurring(input)
    {
        bail!("MAPI meeting upload does not exactly match its hidden placeholder");
    }
    if row.get::<bool, _>("has_identity")
        || row.get::<bool, _>("has_custom_properties")
        || row.get::<bool, _>("has_attachments")
    {
        bail!("MAPI meeting placeholder was projected before adoption");
    }

    let placeholder_attendees_json = row.get::<String, _>("attendees_json");
    let placeholder_participants =
        parse_calendar_participants_metadata(&placeholder_attendees_json);
    let mut imported_participants =
        parse_calendar_participants_metadata(input.event.attendees_json.trim());
    if canonical_organizer_email(
        &placeholder_participants,
        &row.get::<String, _>("organizer_json"),
    )? != canonical_organizer_email(&imported_participants, &input.event.organizer_json)?
        || unique_attendee_emails(&placeholder_participants)?
            != unique_attendee_emails(&imported_participants)?
    {
        bail!("MAPI meeting upload participants do not match its hidden placeholder");
    }

    merge_early_meeting_responses(
        &placeholder_participants,
        &mut imported_participants,
        &row.get::<String, _>("meeting_response_state_json"),
        input.event.sequence,
    )?;
    let attendees_json = serialize_calendar_participants_metadata(&imported_participants);
    let attendee_labels = calendar_attendee_labels(&imported_participants);
    let updated = sqlx::query(
        r#"
        UPDATE calendar_events
        SET calendar_id = $4,
            uid = $5,
            starts_at = (($6::date + $7::time)
                AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC')),
            ends_at = ((($6::date + $7::time)
                AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC'))
                + make_interval(mins => GREATEST($9, 0))),
            time_zone = $8,
            all_day = $10,
            status = COALESCE(NULLIF($11, ''), 'confirmed'),
            sequence = GREATEST($12, 0),
            recurrence_rule = NULLIF($13, ''),
            recurrence_json = CASE
                WHEN NULLIF($14, '') IS NULL THEN '{}'::jsonb ELSE $14::jsonb
            END,
            recurrence_exceptions_json = CASE
                WHEN NULLIF($15, '') IS NULL THEN '[]'::jsonb ELSE $15::jsonb
            END,
            title = $16,
            location = $17,
            organizer_json = CASE
                WHEN NULLIF($18, '') IS NULL THEN '{}'::jsonb ELSE $18::jsonb
            END,
            attendees_json = $19::jsonb,
            body_text = $21,
            body_html = NULLIF($22, ''),
            import_source = 'mapi',
            source_payload_json = jsonb_build_object('attendees', $20::text),
            projection_state = $23,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND id = $3
          AND projection_state = 'mapi_submission_placeholder'
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(event_id)
    .bind(calendar_id)
    .bind(event_uid)
    .bind(input.event.date.trim())
    .bind(input.event.time.trim())
    .bind(input.event.time_zone.trim())
    .bind(input.event.duration_minutes.max(0))
    .bind(input.event.all_day)
    .bind(input.event.status.trim())
    .bind(input.event.sequence)
    .bind(input.event.recurrence_rule.trim())
    .bind(input.event.recurrence_json.trim())
    .bind(input.event.recurrence_exceptions_json.trim())
    .bind(input.event.title.trim())
    .bind(input.event.location.trim())
    .bind(input.event.organizer_json.trim())
    .bind(&attendees_json)
    .bind(attendee_labels)
    .bind(input.event.notes.trim())
    .bind(input.event.body_html.trim())
    .bind(CALENDAR_EVENT_PROJECTION_VISIBLE)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        bail!("MAPI meeting placeholder disappeared during adoption");
    }
    Ok(Some(event_id))
}

fn input_is_non_recurring(input: &MapiEventCreateInput) -> bool {
    input.event.recurrence_rule.trim().is_empty()
        && json_is_empty_object_or_blank(&input.event.recurrence_json)
        && json_is_empty_array_or_blank(&input.event.recurrence_exceptions_json)
}

fn json_is_empty_object_or_blank(raw: &str) -> bool {
    raw.trim().is_empty()
        || serde_json::from_str::<serde_json::Value>(raw)
            .is_ok_and(|value| value.as_object().is_some_and(serde_json::Map::is_empty))
}

fn json_is_empty_array_or_blank(raw: &str) -> bool {
    raw.trim().is_empty()
        || serde_json::from_str::<serde_json::Value>(raw)
            .is_ok_and(|value| value.as_array().is_some_and(Vec::is_empty))
}

fn canonical_organizer_email(
    participants: &CalendarParticipantsMetadata,
    organizer_json: &str,
) -> Result<String> {
    let mut emails = BTreeSet::new();
    if let Some(organizer) = participants.organizer.as_ref() {
        insert_organizer_email(&mut emails, &organizer.email)?;
    }
    let value: serde_json::Value = serde_json::from_str(organizer_json)?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("Calendar organizer metadata is not an object"))?;
    if let Some(email) = object.get("email") {
        insert_organizer_email(
            &mut emails,
            email
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Calendar organizer email is not text"))?,
        )?;
    }
    if let Some(send_to) = object.get("sendTo") {
        insert_organizer_email(
            &mut emails,
            send_to
                .as_object()
                .and_then(|value| value.get("imip"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("Calendar organizer sendTo.imip is invalid"))?,
        )?;
    }
    if emails.len() != 1 {
        bail!("Calendar organizer metadata is missing or inconsistent");
    }
    Ok(emails
        .into_iter()
        .next()
        .expect("one validated organizer email"))
}

fn insert_organizer_email(emails: &mut BTreeSet<String>, value: &str) -> Result<()> {
    let value = value.trim();
    let value = value
        .get(.."mailto:".len())
        .filter(|prefix| prefix.eq_ignore_ascii_case("mailto:"))
        .and_then(|_| value.get("mailto:".len()..))
        .unwrap_or(value);
    let email = normalize_email(value);
    if email.is_empty() {
        bail!("Calendar organizer email is empty");
    }
    emails.insert(email);
    Ok(())
}

fn unique_attendee_emails(participants: &CalendarParticipantsMetadata) -> Result<BTreeSet<String>> {
    let mut emails = BTreeSet::new();
    for attendee in &participants.attendees {
        let email = normalize_email(&attendee.email);
        if email.is_empty() || !emails.insert(email) {
            bail!("Calendar attendee metadata is empty or duplicated");
        }
    }
    if emails.is_empty() {
        bail!("Calendar attendee metadata is empty");
    }
    Ok(emails)
}

fn merge_early_meeting_responses(
    placeholder: &CalendarParticipantsMetadata,
    imported: &mut CalendarParticipantsMetadata,
    response_state_json: &str,
    event_sequence: i32,
) -> Result<()> {
    let response_state = serde_json::from_str::<BTreeMap<String, PlaceholderResponseWatermark>>(
        response_state_json,
    )?;
    for (attendee_key, watermark) in &response_state {
        let normalized_key = normalize_email(attendee_key);
        if normalized_key.is_empty()
            || normalized_key != *attendee_key
            || watermark.sequence != event_sequence
        {
            bail!("Calendar meeting-response watermark is invalid for the imported Event");
        }
        let placeholder_matches = placeholder
            .attendees
            .iter()
            .filter(|attendee| normalize_email(&attendee.email) == normalized_key)
            .collect::<Vec<_>>();
        let mut imported_matches = imported
            .attendees
            .iter_mut()
            .filter(|attendee| normalize_email(&attendee.email) == normalized_key)
            .collect::<Vec<_>>();
        let ([placeholder_attendee], [imported_attendee]) = (
            placeholder_matches.as_slice(),
            imported_matches.as_mut_slice(),
        ) else {
            bail!("Calendar meeting-response watermark lacks one exact attendee");
        };
        imported_attendee.partstat = placeholder_attendee.partstat.clone();
        imported_attendee.counter_proposal = placeholder_attendee.counter_proposal;
        imported_attendee.proposed_start = placeholder_attendee.proposed_start.clone();
        imported_attendee.proposed_end = placeholder_attendee.proposed_end.clone();
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct PlaceholderResponseWatermark {
    sequence: i32,
    #[serde(rename = "sentAt", default)]
    _sent_at: Option<String>,
}

fn calendar_event_uid_advisory_lock_keys(
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    normalized_uid: &str,
) -> (i32, i32) {
    let mut hasher = Sha256::new();
    hasher.update(b"lpe-calendar-event-owner-uid-v1\0");
    hasher.update(tenant_id.as_bytes());
    hasher.update(owner_account_id.as_bytes());
    hasher.update(normalized_uid.as_bytes());
    let digest = hasher.finalize();
    (
        i32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]),
        i32::from_be_bytes([digest[4], digest[5], digest[6], digest[7]]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_uid_advisory_lock_is_stable_and_scoped() {
        let tenant = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let owner = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
        let other_owner = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
        let first = calendar_event_uid_advisory_lock_keys(&tenant, owner, "uid@example.test");
        assert_eq!(
            first,
            calendar_event_uid_advisory_lock_keys(&tenant, owner, "uid@example.test")
        );
        assert_ne!(
            first,
            calendar_event_uid_advisory_lock_keys(&tenant, other_owner, "uid@example.test")
        );
        assert_ne!(
            first,
            calendar_event_uid_advisory_lock_keys(&tenant, owner, "other@example.test")
        );
    }
}
