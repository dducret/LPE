use anyhow::{anyhow, bail, Result};
use sqlx::Postgres;
use uuid::Uuid;

use crate::{normalize_calendar_meeting_uid, UpsertClientEventInput};

use super::lock_calendar_event_uid_in_tx;

pub(super) async fn lock_incoming_mapi_event_uid_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    event_id: Uuid,
    input: Option<&UpsertClientEventInput>,
) -> Result<Option<String>> {
    let Some(input) = input else {
        return Ok(None);
    };
    let uid = if input.uid.trim().is_empty() {
        event_id.to_string()
    } else {
        normalize_calendar_meeting_uid(&input.uid)
    };
    lock_calendar_event_uid_in_tx(tx, tenant_id, input.account_id, &uid).await?;
    Ok(Some(uid))
}

pub(super) async fn ensure_mapi_event_uid_is_not_hidden_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    uid: &str,
) -> Result<()> {
    let hidden_uid_exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND uid = $3
              AND projection_state = 'mapi_submission_placeholder'
        )
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(uid)
    .fetch_one(&mut **tx)
    .await?;
    if hidden_uid_exists {
        bail!("Event UID is reserved by a pending MAPI meeting upload");
    }
    Ok(())
}

pub(super) async fn update_mapi_event_core_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    input: &UpsertClientEventInput,
) -> Result<()> {
    let event_id = input
        .id
        .ok_or_else(|| anyhow!("MAPI Event update requires a canonical Event id"))?;
    let event_uid = if input.uid.trim().is_empty() {
        String::new()
    } else {
        normalize_calendar_meeting_uid(&input.uid)
    };
    let updated = sqlx::query(
        r#"
        UPDATE calendar_events
        SET uid = COALESCE(NULLIF($4, ''), id::text),
            starts_at = (($5::date + $6::time) AT TIME ZONE COALESCE(NULLIF($7, ''), 'UTC')),
            ends_at = ((($5::date + $6::time) AT TIME ZONE COALESCE(NULLIF($7, ''), 'UTC'))
                + make_interval(mins => GREATEST($8, 0))),
            time_zone = $7,
            all_day = $9,
            status = COALESCE(NULLIF($10, ''), 'confirmed'),
            sequence = GREATEST($11, 0),
            recurrence_rule = NULLIF($12, ''),
            recurrence_json = CASE
                WHEN NULLIF($13, '') IS NOT NULL THEN $13::jsonb
                ELSE '{}'::jsonb
            END,
            recurrence_exceptions_json = CASE
                WHEN NULLIF($14, '') IS NOT NULL THEN $14::jsonb
                ELSE '[]'::jsonb
            END,
            title = $15,
            location = $16,
            organizer_json = CASE
                WHEN NULLIF($17, '') IS NOT NULL THEN $17::jsonb
                ELSE '{}'::jsonb
            END,
            attendees_json = CASE
                WHEN NULLIF($19, '') IS NOT NULL THEN $19::jsonb
                WHEN NULLIF($18, '') IS NOT NULL THEN
                    jsonb_build_object(
                        'attendees',
                        jsonb_build_array(jsonb_build_object('email', $18::text))
                    )
                ELSE '{}'::jsonb
            END,
            body_text = $20,
            body_html = NULLIF($21, ''),
            source_payload_json = jsonb_build_object('attendees', $18::text),
            updated_at = NOW()
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND id = $3
          AND lifecycle_state IN ('active', 'deleted')
          AND projection_state = 'visible'
        "#,
    )
    .bind(tenant_id)
    .bind(input.account_id)
    .bind(event_id)
    .bind(event_uid)
    .bind(input.date.trim())
    .bind(input.time.trim())
    .bind(input.time_zone.trim())
    .bind(input.duration_minutes.max(0))
    .bind(input.all_day)
    .bind(input.status.trim())
    .bind(input.sequence)
    .bind(input.recurrence_rule.trim())
    .bind(input.recurrence_json.trim())
    .bind(input.recurrence_exceptions_json.trim())
    .bind(input.title.trim())
    .bind(input.location.trim())
    .bind(input.organizer_json.trim())
    .bind(input.attendees.trim())
    .bind(input.attendees_json.trim())
    .bind(input.notes.trim())
    .bind(input.body_html.trim())
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        bail!("canonical MAPI calendar Event was not updated");
    }
    Ok(())
}
