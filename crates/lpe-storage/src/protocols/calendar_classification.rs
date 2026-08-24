use std::collections::HashMap;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    mapi_message_identity::rotate_active_mapi_message_identity_in_tx, CalendarMeetingRequest,
    CalendarMeetingResponse, Storage,
};

use super::calendar_mail;

// Revision 2 adds transition-aware Microsoft VTIMEZONE parsing. MIME-role
// changes are applied at fresh ingress because the durable selected-part flag
// intentionally cannot be guessed from a detached calendar blob.
pub(crate) const CALENDAR_MAIL_PARSER_REVISION: i32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum DurableCalendarMailMetadata {
    None,
    Request { request: CalendarMeetingRequest },
    Response { response: CalendarMeetingResponse },
}

#[derive(Debug)]
struct StoredCalendarMailClassification {
    classification_generation: i64,
    requires_projection_rotation: bool,
    needs_reclassification: bool,
    classification: String,
    scheduling_mime_part_id: Option<Uuid>,
    metadata_json: Value,
}

impl DurableCalendarMailMetadata {
    fn classification(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Request { .. } => "request",
            Self::Response { .. } => "response",
        }
    }

    fn transport_attachment_id(&self) -> Option<Uuid> {
        match self {
            Self::None => None,
            Self::Request { request } => request.transport_attachment_id,
            Self::Response { response } => response.transport_attachment_id,
        }
    }
}

impl Storage {
    /// Persists the current parser result while a new message is still being
    /// created. The surrounding message-creation transaction already owns the
    /// initial mailbox version, so this path deliberately emits no repair.
    pub(crate) async fn persist_new_calendar_mail_classification_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        message_id: Uuid,
        request: Option<&CalendarMeetingRequest>,
        response: Option<&CalendarMeetingResponse>,
    ) -> Result<()> {
        let metadata = parsed_metadata(request.cloned(), response.cloned());
        let scheduling_mime_part_id =
            resolve_scheduling_mime_part_id_in_tx(tx, tenant_id, account_id, message_id, &metadata)
                .await?;
        let classification = metadata.classification();
        let requires_projection_rotation = classification != "none";
        let metadata_json = serde_json::to_value(metadata)?;
        sqlx::query(
            r#"
            INSERT INTO calendar_mail_classifications (
                tenant_id, message_id, parser_revision, classification_generation,
                requires_projection_rotation, needs_reclassification, classification,
                scheduling_mime_part_id, metadata_json
            )
            VALUES ($1, $2, $3, 1, $4, FALSE, $5, $6, $7)
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(CALENDAR_MAIL_PARSER_REVISION)
        .bind(requires_projection_rotation)
        .bind(classification)
        .bind(scheduling_mime_part_id)
        .bind(metadata_json)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(crate) async fn mark_calendar_mail_classification_applied_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO calendar_mail_classification_projections (
                tenant_id, account_id, message_id, applied_generation
            )
            SELECT tenant_id, $3, message_id, classification_generation
            FROM calendar_mail_classifications
            WHERE tenant_id = $1 AND message_id = $2
              AND NOT needs_reclassification
            ON CONFLICT (tenant_id, account_id, message_id) DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(account_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(crate) async fn mark_calendar_mail_classification_applied_for_first_visible_membership_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<()> {
        let is_first_visible_membership = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT COUNT(*) = 1
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND visibility = 'visible'
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_one(&mut **tx)
        .await?;
        if is_first_visible_membership {
            self.mark_calendar_mail_classification_applied_in_tx(
                tx, tenant_id, account_id, message_id,
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn fetch_or_repair_calendar_mail_metadata(
        &self,
        tenant_id: Uuid,
        account_id: Uuid,
        message_ids: &[Uuid],
    ) -> Result<(
        HashMap<Uuid, CalendarMeetingRequest>,
        HashMap<Uuid, CalendarMeetingResponse>,
    )> {
        for _ in 0..4 {
            let stale_message_ids = sqlx::query_scalar::<_, Uuid>(
                r#"
            SELECT DISTINCT message.id
            FROM messages message
            JOIN mailbox_messages membership
              ON membership.tenant_id = message.tenant_id
             AND membership.message_id = message.id
             AND membership.account_id = $2
             AND membership.visibility = 'visible'
            LEFT JOIN calendar_mail_classifications classification
              ON classification.tenant_id = message.tenant_id
             AND classification.message_id = message.id
            WHERE message.tenant_id = $1
              AND message.id = ANY($3)
              AND (
                    classification.parser_revision IS NULL
                    OR classification.parser_revision <> $4
                    OR classification.needs_reclassification
              )
            ORDER BY message.id
            "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(message_ids)
            .bind(CALENDAR_MAIL_PARSER_REVISION)
            .fetch_all(&self.pool)
            .await?;

            if !stale_message_ids.is_empty() {
                let mut repaired = false;
                for attempt in 0..3 {
                    let (
                        mut parsed_requests,
                        mut parsed_responses,
                        mut parsed_scheduling_parts,
                        mut parsed_fingerprints,
                    ) = calendar_mail::fetch_calendar_mail_metadata(
                        &self.pool,
                        tenant_id,
                        account_id,
                        &stale_message_ids,
                    )
                    .await?;
                    let mut tx = self.pool.begin().await?;
                    let locked_message_ids = sqlx::query_scalar::<_, Uuid>(
                        r#"
                    SELECT id
                    FROM messages
                    WHERE tenant_id = $1
                      AND id = ANY($2)
                    ORDER BY id
                    FOR UPDATE
                    "#,
                    )
                    .bind(tenant_id)
                    .bind(&stale_message_ids)
                    .fetch_all(&mut *tx)
                    .await?;
                    parsed_scheduling_parts
                        .retain(|message_id, _| locked_message_ids.contains(message_id));
                    parsed_fingerprints
                        .retain(|message_id, _| locked_message_ids.contains(message_id));
                    let current_fingerprints =
                        calendar_mail::fetch_calendar_mail_fingerprints_in_tx(
                            &mut tx,
                            tenant_id,
                            account_id,
                            &locked_message_ids,
                        )
                        .await?;
                    if parsed_fingerprints != current_fingerprints {
                        tx.rollback().await?;
                        if attempt == 2 {
                            bail!("calendar MIME authorization state kept changing during classification repair");
                        }
                        continue;
                    }

                    for message_id in locked_message_ids {
                        let existing =
                            load_stored_classification_in_tx(&mut tx, &tenant_id, message_id)
                                .await?;
                        if let Some((revision, stored)) = existing.as_ref() {
                            if *revision > CALENDAR_MAIL_PARSER_REVISION {
                                bail!(
                                "calendar-mail parser revision {revision} is newer than supported revision {CALENDAR_MAIL_PARSER_REVISION}"
                            );
                            }
                            if *revision == CALENDAR_MAIL_PARSER_REVISION
                                && !stored.needs_reclassification
                            {
                                continue;
                            }
                        }

                        let metadata = parsed_metadata(
                            parsed_requests.remove(&message_id),
                            parsed_responses.remove(&message_id),
                        );
                        let scheduling_mime_part_id = match metadata {
                            DurableCalendarMailMetadata::None => None,
                            _ => Some(*parsed_scheduling_parts.get(&message_id).ok_or_else(
                                || {
                                    anyhow::anyhow!(
                                "actionable calendar mail has no exact scheduling MIME part"
                            )
                                },
                            )?),
                        };
                        let classification = metadata.classification();
                        let metadata_json = serde_json::to_value(metadata)?;
                        let requires_rotation = calendar_classification_requires_version(
                            existing.as_ref().map(|(_, stored)| stored),
                            classification,
                            scheduling_mime_part_id,
                            &metadata_json,
                        );
                        let (classification_generation, requires_projection_rotation) =
                            match existing.as_ref() {
                                Some((_, stored)) if requires_rotation => (
                                    stored.classification_generation.checked_add(1).ok_or_else(
                                        || {
                                            anyhow::anyhow!(
                                                "calendar classification generation overflow"
                                            )
                                        },
                                    )?,
                                    true,
                                ),
                                Some((_, stored)) => (
                                    stored.classification_generation,
                                    stored.requires_projection_rotation,
                                ),
                                None => (1, classification != "none"),
                            };

                        if requires_rotation {
                            // Reset retained memberships for every account in
                            // the same generation transaction. Recoverable
                            // restore takes the canonical message lock before
                            // locking its source membership, matching this
                            // message-then-membership order.
                            sqlx::query(
                                r#"
                                UPDATE mailbox_messages
                                SET calendar_request_processed = FALSE,
                                    updated_at = NOW()
                                WHERE tenant_id = $1
                                  AND message_id = $2
                                  AND visibility <> 'visible'
                                "#,
                            )
                            .bind(tenant_id)
                            .bind(message_id)
                            .execute(&mut *tx)
                            .await?;
                        }

                        sqlx::query(
                            r#"
                        INSERT INTO calendar_mail_classifications (
                            tenant_id, message_id, parser_revision,
                            classification_generation, requires_projection_rotation,
                            needs_reclassification, classification,
                            scheduling_mime_part_id, metadata_json
                        )
                        VALUES ($1, $2, $3, $4, $5, FALSE, $6, $7, $8)
                        ON CONFLICT (tenant_id, message_id) DO UPDATE SET
                            parser_revision = EXCLUDED.parser_revision,
                            classification_generation = EXCLUDED.classification_generation,
                            requires_projection_rotation = EXCLUDED.requires_projection_rotation,
                            needs_reclassification = FALSE,
                            classification = EXCLUDED.classification,
                            scheduling_mime_part_id = EXCLUDED.scheduling_mime_part_id,
                            metadata_json = EXCLUDED.metadata_json,
                            updated_at = NOW()
                        "#,
                        )
                        .bind(tenant_id)
                        .bind(message_id)
                        .bind(CALENDAR_MAIL_PARSER_REVISION)
                        .bind(classification_generation)
                        .bind(requires_projection_rotation)
                        .bind(classification)
                        .bind(scheduling_mime_part_id)
                        .bind(&metadata_json)
                        .execute(&mut *tx)
                        .await?;
                    }
                    tx.commit().await?;
                    repaired = true;
                    break;
                }
                if !repaired {
                    bail!("calendar classification repair did not converge");
                }
            }

            self.apply_calendar_mail_classification_projections(tenant_id, message_ids)
                .await?;
            let (ready, requests, responses) =
                load_durable_calendar_mail_metadata(&self.pool, tenant_id, account_id, message_ids)
                    .await?;
            if ready {
                return Ok((requests, responses));
            }
        }
        bail!("calendar classification projections kept changing during metadata read")
    }

    async fn apply_calendar_mail_classification_projections(
        &self,
        tenant_id: Uuid,
        message_ids: &[Uuid],
    ) -> Result<()> {
        let account_ids = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT DISTINCT membership.account_id
            FROM mailbox_messages membership
            JOIN calendar_mail_classifications classification
              ON classification.tenant_id = membership.tenant_id
             AND classification.message_id = membership.message_id
            LEFT JOIN calendar_mail_classification_projections projection
              ON projection.tenant_id = membership.tenant_id
             AND projection.account_id = membership.account_id
             AND projection.message_id = membership.message_id
            WHERE membership.tenant_id = $1
              AND membership.message_id = ANY($2)
              AND membership.visibility = 'visible'
              AND NOT classification.needs_reclassification
              AND projection.applied_generation IS DISTINCT FROM
                  classification.classification_generation
            ORDER BY membership.account_id
            "#,
        )
        .bind(tenant_id)
        .bind(message_ids)
        .fetch_all(&self.pool)
        .await?;

        for account_id in account_ids {
            let mut tx = self.pool.begin().await?;
            let locked_message_ids = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT message.id
                FROM messages message
                JOIN calendar_mail_classifications classification
                  ON classification.tenant_id = message.tenant_id
                 AND classification.message_id = message.id
                WHERE message.tenant_id = $1
                  AND message.id = ANY($2)
                  AND NOT classification.needs_reclassification
                  AND EXISTS (
                        SELECT 1
                        FROM mailbox_messages membership
                        WHERE membership.tenant_id = message.tenant_id
                          AND membership.account_id = $3
                          AND membership.message_id = message.id
                          AND membership.visibility = 'visible'
                  )
                  AND NOT EXISTS (
                        SELECT 1
                        FROM calendar_mail_classification_projections projection
                        WHERE projection.tenant_id = message.tenant_id
                          AND projection.account_id = $3
                          AND projection.message_id = message.id
                          AND projection.applied_generation =
                              classification.classification_generation
                  )
                ORDER BY message.id
                FOR UPDATE OF message
                "#,
            )
            .bind(tenant_id)
            .bind(message_ids)
            .bind(account_id)
            .fetch_all(&mut *tx)
            .await?;
            lock_account_mail_sync_state_in_tx(&mut tx, &tenant_id, account_id).await?;

            let mut emitted_change = false;
            for message_id in locked_message_ids {
                let row = sqlx::query(
                    r#"
                    SELECT classification, classification_generation,
                           requires_projection_rotation
                    FROM calendar_mail_classifications classification
                    WHERE tenant_id = $1
                      AND message_id = $2
                      AND NOT needs_reclassification
                      AND EXISTS (
                            SELECT 1
                            FROM mailbox_messages membership
                            WHERE membership.tenant_id = classification.tenant_id
                              AND membership.account_id = $3
                              AND membership.message_id = classification.message_id
                              AND membership.visibility = 'visible'
                      )
                      AND NOT EXISTS (
                            SELECT 1
                            FROM calendar_mail_classification_projections projection
                            WHERE projection.tenant_id = classification.tenant_id
                              AND projection.account_id = $3
                              AND projection.message_id = classification.message_id
                              AND projection.applied_generation =
                                  classification.classification_generation
                      )
                    "#,
                )
                .bind(tenant_id)
                .bind(message_id)
                .bind(account_id)
                .fetch_optional(&mut *tx)
                .await?;
                let Some(row) = row else {
                    continue;
                };
                let classification: String = row.try_get("classification")?;
                let generation: i64 = row.try_get("classification_generation")?;
                let requires_rotation: bool = row.try_get("requires_projection_rotation")?;
                if requires_rotation
                    && version_calendar_classification_repair_in_tx(
                        self,
                        &mut tx,
                        &tenant_id,
                        account_id,
                        message_id,
                        &classification,
                        generation,
                    )
                    .await?
                {
                    emitted_change = true;
                }
                sqlx::query(
                    r#"
                    INSERT INTO calendar_mail_classification_projections (
                        tenant_id, account_id, message_id, applied_generation
                    )
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (tenant_id, account_id, message_id) DO UPDATE SET
                        applied_generation = EXCLUDED.applied_generation,
                        updated_at = NOW()
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(message_id)
                .bind(generation)
                .execute(&mut *tx)
                .await?;
            }
            if emitted_change {
                Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
            }
            tx.commit().await?;
        }
        Ok(())
    }
}

fn parsed_metadata(
    request: Option<CalendarMeetingRequest>,
    response: Option<CalendarMeetingResponse>,
) -> DurableCalendarMailMetadata {
    if let Some(request) = request {
        DurableCalendarMailMetadata::Request { request }
    } else if let Some(response) = response {
        DurableCalendarMailMetadata::Response { response }
    } else {
        DurableCalendarMailMetadata::None
    }
}

async fn load_stored_classification_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    message_id: Uuid,
) -> Result<Option<(i32, StoredCalendarMailClassification)>> {
    let row = sqlx::query(
        r#"
        SELECT parser_revision, classification_generation, requires_projection_rotation,
               needs_reclassification, classification, scheduling_mime_part_id, metadata_json
        FROM calendar_mail_classifications
        WHERE tenant_id = $1 AND message_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(message_id)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(|row| {
        Ok((
            row.try_get("parser_revision")?,
            StoredCalendarMailClassification {
                classification_generation: row.try_get("classification_generation")?,
                requires_projection_rotation: row.try_get("requires_projection_rotation")?,
                needs_reclassification: row.try_get("needs_reclassification")?,
                classification: row.try_get("classification")?,
                scheduling_mime_part_id: row.try_get("scheduling_mime_part_id")?,
                metadata_json: row.try_get("metadata_json")?,
            },
        ))
    })
    .transpose()
}

async fn resolve_scheduling_mime_part_id_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    message_id: Uuid,
    metadata: &DurableCalendarMailMetadata,
) -> Result<Option<Uuid>> {
    if matches!(metadata, DurableCalendarMailMetadata::None) {
        return Ok(None);
    }
    if let Some(attachment_id) = metadata.transport_attachment_id() {
        let mime_part_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT part.id
            FROM attachments attachment
            JOIN mime_parts part
              ON part.tenant_id = attachment.tenant_id
             AND part.message_id = attachment.message_id
             AND part.id = attachment.mime_part_id
            WHERE attachment.tenant_id = $1
              AND attachment.account_id = $2
              AND attachment.message_id = $3
              AND attachment.id = $4
              AND part.is_scheduling_body
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(attachment_id)
        .fetch_optional(&mut **tx)
        .await?;
        if mime_part_id.is_some() {
            return Ok(mime_part_id);
        }
    }
    let mime_part_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM mime_parts
        WHERE tenant_id = $1
          AND message_id = $2
          AND is_scheduling_body
        ORDER BY ordinal, id
        LIMIT 1
        "#,
    )
    .bind(tenant_id)
    .bind(message_id)
    .fetch_optional(&mut **tx)
    .await?;
    if mime_part_id.is_none() {
        bail!("actionable calendar mail has no exact scheduling MIME part");
    }
    Ok(mime_part_id)
}

fn calendar_classification_requires_version(
    existing: Option<&StoredCalendarMailClassification>,
    classification: &str,
    scheduling_mime_part_id: Option<Uuid>,
    metadata_json: &Value,
) -> bool {
    let Some(existing) = existing else {
        return classification != "none";
    };
    let payload_changed = existing.classification != classification
        || existing.scheduling_mime_part_id != scheduling_mime_part_id
        || existing.metadata_json != *metadata_json;
    payload_changed && (existing.classification != "none" || classification != "none")
}

async fn version_calendar_classification_repair_in_tx(
    storage: &Storage,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    message_id: Uuid,
    classification: &str,
    generation: i64,
) -> Result<bool> {
    let has_visible_membership = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND visibility = 'visible'
        )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_id)
    .fetch_one(&mut **tx)
    .await?;
    if !has_visible_membership {
        return Ok(false);
    }
    let modseq = storage
        .allocate_mail_modseq_in_tx(tx, tenant_id, account_id)
        .await?;
    let memberships = sqlx::query(
        r#"
        UPDATE mailbox_messages
        SET calendar_request_processed = FALSE,
            modseq = $4,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND visibility = 'visible'
        RETURNING id, mailbox_id, COALESCE(thread_id, message_id) AS thread_id, imap_uid
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_id)
    .bind(modseq)
    .fetch_all(&mut **tx)
    .await?;
    if memberships.is_empty() {
        return Ok(false);
    }

    rotate_active_mapi_message_identity_in_tx(tx, tenant_id, account_id, message_id).await?;
    let principals = Storage::affected_mail_principals_in_tx(tx, tenant_id, account_id).await?;
    Storage::insert_mail_change_log_in_tx(
        tx,
        tenant_id,
        Some(account_id),
        None,
        "message",
        message_id,
        "updated",
        modseq,
        &principals,
        serde_json::json!({
            "messageId": message_id,
            "calendarClassification": classification,
            "parserRevision": CALENDAR_MAIL_PARSER_REVISION,
            "classificationGeneration": generation,
            "calendarRequestProcessedReset": true
        }),
    )
    .await?;
    for membership in memberships {
        Storage::insert_mail_change_log_in_tx(
            tx,
            tenant_id,
            Some(account_id),
            Some(membership.try_get("mailbox_id")?),
            "mailbox_message",
            membership.try_get("id")?,
            "updated",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "threadId": membership.try_get::<Uuid, _>("thread_id")?,
                "imapUid": membership.try_get::<i64, _>("imap_uid")?,
                "calendarClassification": classification,
                "parserRevision": CALENDAR_MAIL_PARSER_REVISION,
                "classificationGeneration": generation,
                "calendarRequestProcessedReset": true
            }),
        )
        .await?;
    }
    Ok(true)
}

async fn lock_account_mail_sync_state_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO account_sync_state (tenant_id, account_id, category, current_modseq)
        VALUES ($1, $2, 'mail', 1)
        ON CONFLICT (tenant_id, account_id, category) DO NOTHING
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT current_modseq
        FROM account_sync_state
        WHERE tenant_id = $1 AND account_id = $2 AND category = 'mail'
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(())
}

async fn load_durable_calendar_mail_metadata(
    pool: &sqlx::PgPool,
    tenant_id: Uuid,
    account_id: Uuid,
    message_ids: &[Uuid],
) -> Result<(
    bool,
    HashMap<Uuid, CalendarMeetingRequest>,
    HashMap<Uuid, CalendarMeetingResponse>,
)> {
    let rows = sqlx::query(
        r#"
        WITH requested_messages AS (
            SELECT
                membership.tenant_id,
                membership.message_id,
                bool_and(membership.calendar_request_processed) AS calendar_request_processed
            FROM mailbox_messages membership
            WHERE membership.tenant_id = $1
              AND membership.account_id = $2
              AND membership.message_id = ANY($3)
              AND membership.visibility = 'visible'
            GROUP BY membership.tenant_id, membership.message_id
        )
        SELECT
            requested.message_id,
            classification.classification,
            classification.metadata_json,
            canonical_message.authorized_calendar_response_content_sha256,
            canonical_message.calendar_response_processed,
            requested.calendar_request_processed,
            scheduling_blob.content_sha256 AS scheduling_content_sha256,
            transport_attachment.id AS transport_attachment_id,
            COALESCE(
                classification.parser_revision = $4
                AND NOT classification.needs_reclassification
                AND NOT EXISTS (
                    SELECT 1
                    FROM mailbox_messages visible_membership
                    LEFT JOIN calendar_mail_classification_projections projection
                      ON projection.tenant_id = visible_membership.tenant_id
                     AND projection.account_id = visible_membership.account_id
                     AND projection.message_id = visible_membership.message_id
                    WHERE visible_membership.tenant_id = requested.tenant_id
                      AND visible_membership.message_id = requested.message_id
                      AND visible_membership.visibility = 'visible'
                      AND projection.applied_generation IS DISTINCT FROM
                          classification.classification_generation
                ),
                FALSE
            ) AS projection_ready
        FROM requested_messages requested
        JOIN messages canonical_message
          ON canonical_message.tenant_id = requested.tenant_id
         AND canonical_message.id = requested.message_id
        LEFT JOIN calendar_mail_classifications classification
          ON classification.tenant_id = requested.tenant_id
         AND classification.message_id = requested.message_id
        LEFT JOIN mime_parts scheduling_part
          ON scheduling_part.tenant_id = requested.tenant_id
         AND scheduling_part.message_id = requested.message_id
         AND scheduling_part.id = classification.scheduling_mime_part_id
        LEFT JOIN blobs scheduling_blob
          ON scheduling_blob.tenant_id = scheduling_part.tenant_id
         AND scheduling_blob.domain_id = scheduling_part.domain_id
         AND scheduling_blob.id = scheduling_part.blob_id
         AND scheduling_blob.blob_kind = scheduling_part.blob_kind
        LEFT JOIN attachments transport_attachment
          ON transport_attachment.tenant_id = requested.tenant_id
         AND transport_attachment.account_id = $2
         AND transport_attachment.message_id = requested.message_id
         AND transport_attachment.mime_part_id = classification.scheduling_mime_part_id
        ORDER BY requested.message_id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_ids)
    .bind(CALENDAR_MAIL_PARSER_REVISION)
    .fetch_all(pool)
    .await?;
    let mut requests = HashMap::new();
    let mut responses = HashMap::new();
    let mut all_ready = true;
    for row in rows {
        let message_id: Uuid = row.try_get("message_id")?;
        if !row.try_get::<bool, _>("projection_ready")? {
            all_ready = false;
            continue;
        }
        let classification: String = row.try_get("classification")?;
        let metadata: DurableCalendarMailMetadata =
            serde_json::from_value(row.try_get("metadata_json")?)?;
        if classification != metadata.classification() {
            bail!("stored calendar mail classification does not match its metadata");
        }
        let transport_attachment_id: Option<Uuid> = row.try_get("transport_attachment_id")?;
        match metadata {
            DurableCalendarMailMetadata::None => {}
            DurableCalendarMailMetadata::Request { mut request } => {
                request.transport_attachment_id = transport_attachment_id;
                request.client_processed = row.try_get("calendar_request_processed")?;
                requests.insert(message_id, request);
            }
            DurableCalendarMailMetadata::Response { mut response } => {
                let authorized_content_sha256: Option<String> =
                    row.try_get("authorized_calendar_response_content_sha256")?;
                let scheduling_content_sha256: Option<String> =
                    row.try_get("scheduling_content_sha256")?;
                let Some(server_processed) = canonical_response_processed(
                    authorized_content_sha256.as_deref(),
                    scheduling_content_sha256.as_deref(),
                    row.try_get("calendar_response_processed")?,
                ) else {
                    continue;
                };
                response.transport_attachment_id = transport_attachment_id;
                response.server_processed = server_processed;
                responses.insert(message_id, response);
            }
        }
    }
    Ok((all_ready, requests, responses))
}

fn canonical_response_processed(
    authorized_content_sha256: Option<&str>,
    scheduling_content_sha256: Option<&str>,
    processed: bool,
) -> Option<bool> {
    (authorized_content_sha256.is_some() && authorized_content_sha256 == scheduling_content_sha256)
        .then_some(processed)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        calendar_classification_requires_version, canonical_response_processed,
        DurableCalendarMailMetadata, StoredCalendarMailClassification,
    };
    use crate::CalendarMeetingRequest;

    fn stored(
        classification: &str,
        scheduling_mime_part_id: Option<Uuid>,
        metadata_json: serde_json::Value,
    ) -> StoredCalendarMailClassification {
        StoredCalendarMailClassification {
            classification_generation: 1,
            requires_projection_rotation: classification != "none",
            needs_reclassification: false,
            classification: classification.to_string(),
            scheduling_mime_part_id,
            metadata_json,
        }
    }

    #[test]
    fn durable_response_reads_use_authoritative_message_processed_state() {
        let hash = "1".repeat(64);
        let other_hash = "2".repeat(64);
        assert_eq!(
            canonical_response_processed(Some(&hash), Some(&hash), false),
            Some(false)
        );
        assert_eq!(
            canonical_response_processed(Some(&hash), Some(&hash), true),
            Some(true)
        );
        assert_eq!(canonical_response_processed(None, Some(&hash), true), None);
        assert_eq!(canonical_response_processed(Some(&hash), None, true), None);
        assert_eq!(
            canonical_response_processed(Some(&hash), Some(&other_hash), true,),
            None
        );
    }

    #[test]
    fn actionable_classification_repairs_rotate_only_for_payload_changes() {
        let part_id = Uuid::new_v4();
        let request = json!({"kind":"request","request":{"uid":"probe"}});
        assert!(calendar_classification_requires_version(
            None,
            "request",
            Some(part_id),
            &request
        ));
        assert!(!calendar_classification_requires_version(
            None,
            "none",
            None,
            &json!({"kind":"none"})
        ));
        let unchanged = stored("request", Some(part_id), request.clone());
        assert!(!calendar_classification_requires_version(
            Some(&unchanged),
            "request",
            Some(part_id),
            &request
        ));
        assert!(calendar_classification_requires_version(
            Some(&unchanged),
            "request",
            Some(part_id),
            &json!({"kind":"request","request":{"uid":"probe-updated"}})
        ));
        assert!(calendar_classification_requires_version(
            Some(&unchanged),
            "none",
            None,
            &json!({"kind":"none"})
        ));
        let none = stored("none", None, json!({"kind":"none"}));
        assert!(!calendar_classification_requires_version(
            Some(&none),
            "none",
            None,
            &json!({"kind":"none"})
        ));
        assert!(calendar_classification_requires_version(
            Some(&none),
            "response",
            Some(part_id),
            &json!({"kind":"response","response":{"uid":"probe"}})
        ));
    }

    #[test]
    fn durable_metadata_omits_account_scoped_request_projection_state() {
        let metadata = DurableCalendarMailMetadata::Request {
            request: CalendarMeetingRequest {
                uid: "probe".to_string(),
                transport_attachment_id: Some(Uuid::new_v4()),
                client_processed: true,
                organizer: None,
                attendees: Vec::new(),
                response_requested: false,
                sent_at: None,
                meeting_start: "2026-08-24T08:00:00Z".to_string(),
                meeting_end: "2026-08-24T08:30:00Z".to_string(),
                meeting_location: None,
                meeting_sequence: 0,
                intended_busy_status: 2,
            },
        };
        let serialized = serde_json::to_value(metadata).unwrap();
        assert_eq!(serialized["kind"], "request");
        assert!(serialized["request"]
            .get("transport_attachment_id")
            .is_none());
        assert!(serialized["request"].get("client_processed").is_none());
    }
}
