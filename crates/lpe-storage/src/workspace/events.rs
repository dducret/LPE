use anyhow::{bail, Result};
use uuid::Uuid;

use crate::{
    external_calendar_uid, normalize_calendar_meeting_uid, CanonicalChangeCategory, ClientEventRow,
    Storage,
};

use super::{json_text_matches, ClientEvent, UpsertClientEventInput};

impl Storage {
    pub async fn upsert_client_event(&self, input: UpsertClientEventInput) -> Result<ClientEvent> {
        self.upsert_client_event_in_calendar(input, None).await
    }

    pub(crate) async fn upsert_client_event_in_calendar(
        &self,
        input: UpsertClientEventInput,
        calendar_id: Option<Uuid>,
    ) -> Result<ClientEvent> {
        let mut input = input;
        if !input.uid.trim().is_empty() {
            input.uid = normalize_calendar_meeting_uid(&input.uid);
        }
        if input.date.trim().is_empty()
            || input.time.trim().is_empty()
            || input.title.trim().is_empty()
        {
            bail!("event date, time, and title are required");
        }

        let event_id = input.id.unwrap_or_else(Uuid::new_v4);
        if input.id.is_some() {
            if let Some(existing) = self
                .fetch_client_events_by_ids(input.account_id, &[event_id])
                .await?
                .into_iter()
                .next()
            {
                if event_update_is_unchanged(&existing, &input, event_id) {
                    return Ok(existing);
                }
            }
        }
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        let event_uid = if input.uid.trim().is_empty() {
            event_id.to_string()
        } else {
            input.uid.clone()
        };
        crate::mapi_events::lock_calendar_event_uid_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            &event_uid,
        )
        .await?;
        let calendar_id = match calendar_id {
            Some(calendar_id) => calendar_id,
            None => {
                Self::ensure_default_calendar_in_tx(&mut tx, &tenant_id, input.account_id).await?
            }
        };
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
        .bind(input.account_id)
        .bind(&event_uid)
        .fetch_one(&mut *tx)
        .await?;
        if hidden_uid_exists {
            bail!("event UID is reserved by a pending MAPI meeting upload");
        }
        let row = sqlx::query_as::<_, ClientEventRow>(
            r#"
            INSERT INTO calendar_events (
                id, tenant_id, owner_account_id, calendar_id, uid,
                starts_at, ends_at, time_zone, all_day, status, sequence,
                recurrence_rule, recurrence_json, recurrence_exceptions_json,
                title, location, organizer_json, attendees_json, body_text, body_html,
                source_payload_json
            )
            VALUES (
                $1, $2, $3, $4, COALESCE(NULLIF($5, ''), $1::text),
                (($6::date + $7::time) AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC')),
                ((($6::date + $7::time) AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC')) + make_interval(mins => GREATEST($9, 0))),
                $8,
                $10,
                COALESCE(NULLIF($11, ''), 'confirmed'),
                GREATEST($12, 0),
                NULLIF($13, ''),
                CASE
                    WHEN NULLIF($14, '') IS NOT NULL THEN $14::jsonb
                    ELSE '{}'::jsonb
                END,
                CASE
                    WHEN NULLIF($15, '') IS NOT NULL THEN $15::jsonb
                    ELSE '[]'::jsonb
                END,
                $16,
                $17,
                CASE
                    WHEN NULLIF($18, '') IS NOT NULL THEN $18::jsonb
                    ELSE '{}'::jsonb
                END,
                CASE
                    WHEN NULLIF($20, '') IS NOT NULL THEN $20::jsonb
                    WHEN NULLIF($19, '') IS NOT NULL THEN jsonb_build_object('attendees', jsonb_build_array(jsonb_build_object('email', $19::text)))
                    ELSE '{}'::jsonb
                END,
                $21,
                NULLIF($22, ''),
                jsonb_build_object('attendees', $19::text)
            )
            ON CONFLICT (id) DO UPDATE SET
                calendar_id = EXCLUDED.calendar_id,
                uid = EXCLUDED.uid,
                starts_at = EXCLUDED.starts_at,
                ends_at = EXCLUDED.ends_at,
                time_zone = EXCLUDED.time_zone,
                all_day = EXCLUDED.all_day,
                status = EXCLUDED.status,
                sequence = EXCLUDED.sequence,
                recurrence_rule = EXCLUDED.recurrence_rule,
                recurrence_json = EXCLUDED.recurrence_json,
                recurrence_exceptions_json = EXCLUDED.recurrence_exceptions_json,
                title = EXCLUDED.title,
                location = EXCLUDED.location,
                organizer_json = EXCLUDED.organizer_json,
                attendees_json = EXCLUDED.attendees_json,
                body_text = EXCLUDED.body_text,
                body_html = EXCLUDED.body_html,
                source_payload_json = EXCLUDED.source_payload_json,
                updated_at = NOW()
            WHERE calendar_events.tenant_id = EXCLUDED.tenant_id
              AND calendar_events.owner_account_id = EXCLUDED.owner_account_id
              AND calendar_events.lifecycle_state = 'active'
              AND calendar_events.projection_state = 'visible'
            RETURNING
                id,
                uid,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'YYYY-MM-DD') AS date,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'HH24:MI') AS time,
                time_zone,
                GREATEST(0, EXTRACT(EPOCH FROM (ends_at - starts_at))::int / 60) AS duration_minutes,
                all_day,
                status,
                sequence,
                COALESCE(recurrence_rule, '') AS recurrence_rule,
                recurrence_json::text AS recurrence_json,
                recurrence_exceptions_json::text AS recurrence_exceptions_json,
                title,
                location,
                organizer_json::text AS organizer_json,
                COALESCE(source_payload_json->>'attendees', '') AS attendees,
                attendees_json::text AS attendees_json,
                body_text AS notes,
                COALESCE(body_html, '') AS body_html,
                (xmax = 0) AS created
            "#,
        )
        .bind(event_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(calendar_id)
        .bind(input.uid.trim())
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
        .fetch_one(&mut *tx)
        .await?;

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        self.advance_calendar_event_version_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            event_id,
            modseq,
        )
        .await?;
        let affected_principals = Self::calendar_event_affected_principals_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            event_id,
        )
        .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            None,
            "calendar_event",
            event_id,
            if row.created == Some(true) {
                "created"
            } else {
                "updated"
            },
            modseq,
            &affected_principals,
            serde_json::json!({
                "collectionId": calendar_id,
                "objectUid": row.uid.clone(),
            }),
        )
        .await?;

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            input.account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(map_event(row))
    }

    pub async fn fetch_client_events(&self, account_id: Uuid) -> Result<Vec<ClientEvent>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ClientEventRow>(
            r#"
            SELECT
                id,
                uid,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'YYYY-MM-DD') AS date,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'HH24:MI') AS time,
                time_zone,
                GREATEST(0, EXTRACT(EPOCH FROM (ends_at - starts_at))::int / 60) AS duration_minutes,
                all_day,
                status,
                sequence,
                COALESCE(recurrence_rule, '') AS recurrence_rule,
                recurrence_json::text AS recurrence_json,
                recurrence_exceptions_json::text AS recurrence_exceptions_json,
                title,
                location,
                organizer_json::text AS organizer_json,
                COALESCE(source_payload_json->>'attendees', '') AS attendees,
                attendees_json::text AS attendees_json,
                body_text AS notes,
                COALESCE(body_html, '') AS body_html
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND lifecycle_state = 'active'
              AND projection_state = 'visible'
            ORDER BY starts_at ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_event).collect())
    }

    pub async fn fetch_client_events_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientEvent>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query_as::<_, ClientEventRow>(
            r#"
            SELECT
                id,
                uid,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'YYYY-MM-DD') AS date,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'HH24:MI') AS time,
                time_zone,
                GREATEST(0, EXTRACT(EPOCH FROM (ends_at - starts_at))::int / 60) AS duration_minutes,
                all_day,
                status,
                sequence,
                COALESCE(recurrence_rule, '') AS recurrence_rule,
                recurrence_json::text AS recurrence_json,
                recurrence_exceptions_json::text AS recurrence_exceptions_json,
                title,
                location,
                organizer_json::text AS organizer_json,
                COALESCE(source_payload_json->>'attendees', '') AS attendees,
                attendees_json::text AS attendees_json,
                body_text AS notes,
                COALESCE(body_html, '') AS body_html
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = ANY($3)
              AND lifecycle_state = 'active'
              AND projection_state = 'visible'
            ORDER BY starts_at ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_event).collect())
    }
}

fn event_update_is_unchanged(
    existing: &ClientEvent,
    input: &UpsertClientEventInput,
    event_id: Uuid,
) -> bool {
    existing.uid
        == if input.uid.trim().is_empty() {
            event_id.to_string()
        } else {
            input.uid.trim().to_string()
        }
        && existing.date == input.date.trim()
        && existing.time == input.time.trim()
        && existing.time_zone == input.time_zone.trim()
        && existing.duration_minutes == input.duration_minutes.max(0)
        && existing.all_day == input.all_day
        && existing.status
            == if input.status.trim().is_empty() {
                "confirmed"
            } else {
                input.status.trim()
            }
        && existing.sequence == input.sequence.max(0)
        && existing.recurrence_rule == input.recurrence_rule.trim()
        && json_text_matches(&existing.recurrence_json, input.recurrence_json.trim())
        && json_text_matches(
            &existing.recurrence_exceptions_json,
            input.recurrence_exceptions_json.trim(),
        )
        && existing.title == input.title.trim()
        && existing.location == input.location.trim()
        && json_text_matches(&existing.organizer_json, input.organizer_json.trim())
        && existing.attendees == input.attendees.trim()
        && json_text_matches(&existing.attendees_json, input.attendees_json.trim())
        && existing.notes == input.notes.trim()
        && existing.body_html == input.body_html.trim()
}

fn map_event(row: ClientEventRow) -> ClientEvent {
    ClientEvent {
        id: row.id,
        uid: external_calendar_uid(&row.uid),
        date: row.date,
        time: row.time,
        time_zone: row.time_zone,
        duration_minutes: row.duration_minutes,
        all_day: row.all_day,
        status: row.status,
        sequence: row.sequence,
        recurrence_rule: row.recurrence_rule,
        recurrence_json: row.recurrence_json,
        recurrence_exceptions_json: row.recurrence_exceptions_json,
        title: row.title,
        location: row.location,
        organizer_json: row.organizer_json,
        attendees: row.attendees,
        attendees_json: row.attendees_json,
        notes: row.notes,
        body_html: row.body_html,
    }
}
