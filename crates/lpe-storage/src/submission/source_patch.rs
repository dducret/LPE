use std::collections::HashSet;

use anyhow::{bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use super::{source::ClaimedSubmissionSource, SubmissionSourcePatch};
use crate::Storage;

fn validate_submission_source_patch(patch: &SubmissionSourcePatch) -> Result<()> {
    let mut attachment_ids = HashSet::new();
    if patch
        .delete_attachment_ids
        .iter()
        .any(|id| id.is_nil() || !attachment_ids.insert(*id))
    {
        bail!("submission source patch contains invalid or duplicate attachment ids");
    }

    let mut upsert_tags = HashSet::new();
    for value in &patch.custom_property_upserts {
        if value.property_tag >> 16 < 0x8000
            || value.property_type == 0
            || value.property_type != value.property_tag as u16
            || !upsert_tags.insert(value.property_tag)
        {
            bail!("submission source patch contains an invalid custom Message property");
        }
    }
    let mut delete_tags = HashSet::new();
    for property_tag in &patch.delete_custom_property_tags {
        if property_tag >> 16 < 0x8000
            || *property_tag as u16 == 0
            || !delete_tags.insert(*property_tag)
            || upsert_tags.contains(property_tag)
        {
            bail!("submission source patch contains an invalid custom Message property delete");
        }
    }
    Ok(())
}

impl Storage {
    pub(super) async fn apply_claimed_submission_source_patch_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        claim: &ClaimedSubmissionSource,
        patch: &SubmissionSourcePatch,
    ) -> Result<()> {
        if patch
            .expected_source_modseq
            .is_some_and(|expected| expected != claim.modseq)
        {
            bail!("submission source changed after the client snapshot");
        }
        validate_submission_source_patch(patch)?;
        if !patch.delete_attachment_ids.is_empty() {
            self.delete_claimed_submission_source_attachments_in_tx(
                tx,
                tenant_id,
                claim,
                Some(&patch.delete_attachment_ids),
            )
            .await?;
        }

        if !patch.delete_custom_property_tags.is_empty() {
            let tags = patch
                .delete_custom_property_tags
                .iter()
                .map(|tag| i64::from(*tag))
                .collect::<Vec<_>>();
            sqlx::query(
                r#"
                DELETE FROM mapi_custom_property_values
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND object_kind = 'message'
                  AND canonical_id = $3
                  AND property_tag = ANY($4)
                "#,
            )
            .bind(tenant_id)
            .bind(claim.account_id)
            .bind(claim.message_id)
            .bind(&tags)
            .execute(&mut **tx)
            .await?;
        }
        if !patch.custom_property_upserts.is_empty() {
            for value in &patch.custom_property_upserts {
                sqlx::query(
                    r#"
                    INSERT INTO mapi_custom_property_values (
                        tenant_id, account_id, object_kind, canonical_id,
                        property_tag, property_type, property_value
                    )
                    VALUES ($1, $2, 'message', $3, $4, $5, $6)
                    ON CONFLICT (
                        tenant_id, account_id, object_kind, canonical_id,
                        property_tag, property_type
                    )
                    DO UPDATE SET
                        property_value = EXCLUDED.property_value,
                        updated_at = NOW()
                    "#,
                )
                .bind(tenant_id)
                .bind(claim.account_id)
                .bind(claim.message_id)
                .bind(i64::from(value.property_tag))
                .bind(i32::from(value.property_type))
                .bind(&value.property_value)
                .execute(&mut **tx)
                .await?;
            }
        }
        if let Some(update) = patch.canonical_followup_update.as_ref() {
            self.apply_claimed_submission_source_followup_in_tx(tx, tenant_id, claim, update)
                .await?;
        }
        Ok(())
    }

    async fn apply_claimed_submission_source_followup_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        claim: &ClaimedSubmissionSource,
        update: &crate::JmapEmailFollowupUpdate,
    ) -> Result<()> {
        crate::mail_followup::validate_followup_update(update)?;
        let categories = update
            .categories
            .clone()
            .map(crate::mail_followup::normalize_mail_categories);
        let updated = sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET is_seen = CASE WHEN $5::bool IS NULL THEN is_seen ELSE NOT $5 END,
                is_flagged = CASE
                    WHEN $6::bool IS NOT NULL THEN $6
                    WHEN $7::text IS NULL THEN is_flagged
                    ELSE $7 IN ('flagged', 'complete')
                END,
                followup_flag_status = COALESCE($7, followup_flag_status),
                followup_icon = CASE
                    WHEN $7 = 'none' THEN 0
                    WHEN $8::integer IS NOT NULL THEN $8
                    WHEN $7 = 'flagged' AND followup_icon = 0 THEN 6
                    ELSE followup_icon
                END,
                todo_item_flags = CASE
                    WHEN $7 = 'none' THEN 0
                    WHEN $9::integer IS NOT NULL THEN $9
                    WHEN $7 IN ('flagged', 'complete') AND todo_item_flags = 0 THEN 8
                    ELSE todo_item_flags
                END,
                followup_request = COALESCE($10, followup_request),
                followup_start_at = CASE
                    WHEN $7 = 'none' THEN NULL
                    WHEN $11::text = '' THEN NULL
                    WHEN $11::text IS NOT NULL THEN $11::timestamptz
                    ELSE followup_start_at
                END,
                followup_due_at = CASE
                    WHEN $7 = 'none' THEN NULL
                    WHEN $12::text = '' THEN NULL
                    WHEN $12::text IS NOT NULL THEN $12::timestamptz
                    ELSE followup_due_at
                END,
                followup_completed_at = CASE
                    WHEN $7 IN ('none', 'flagged') THEN NULL
                    WHEN $13::text IS NOT NULL THEN $13::timestamptz
                    WHEN $7 = 'complete' THEN COALESCE(followup_completed_at, NOW())
                    ELSE followup_completed_at
                END,
                reminder_set = CASE
                    WHEN $7 = 'none' THEN FALSE
                    WHEN $14::bool IS NOT NULL THEN $14
                    ELSE reminder_set
                END,
                reminder_at = CASE
                    WHEN $7 = 'none' THEN NULL
                    WHEN $15::text = '' THEN NULL
                    WHEN $15::text IS NOT NULL THEN $15::timestamptz
                    ELSE reminder_at
                END,
                reminder_dismissed_at = CASE
                    WHEN $7 = 'none' THEN NULL
                    WHEN $16::text = '' THEN NULL
                    WHEN $16::text IS NOT NULL THEN $16::timestamptz
                    ELSE reminder_dismissed_at
                END,
                swapped_todo_store_id = COALESCE($17, swapped_todo_store_id),
                swapped_todo_data = COALESCE($18, swapped_todo_data),
                keywords = COALESCE($19, keywords),
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = $3
              AND message_id = $4
              AND visibility = 'visible'
            "#,
        )
        .bind(tenant_id)
        .bind(claim.account_id)
        .bind(claim.membership_id)
        .bind(claim.message_id)
        .bind(update.unread)
        .bind(update.flagged)
        .bind(update.followup_flag_status.as_deref())
        .bind(update.followup_icon)
        .bind(update.todo_item_flags)
        .bind(update.followup_request.as_deref())
        .bind(update.followup_start_at.as_deref())
        .bind(update.followup_due_at.as_deref())
        .bind(update.followup_completed_at.as_deref())
        .bind(update.reminder_set)
        .bind(update.reminder_at.as_deref())
        .bind(update.reminder_dismissed_at.as_deref())
        .bind(update.swapped_todo_store_id)
        .bind(update.swapped_todo_data.as_deref())
        .bind(categories)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            bail!("claimed submission source changed during follow-up update");
        }
        Ok(())
    }

    pub(super) async fn delete_claimed_submission_source_attachments_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        claim: &ClaimedSubmissionSource,
        attachment_ids: Option<&[Uuid]>,
    ) -> Result<()> {
        self.lock_message_for_mime_graph_in_tx(tx, tenant_id, claim.message_id)
            .await?;
        let delete_all = attachment_ids.is_none();
        let requested_ids = attachment_ids.unwrap_or_default();
        let rows = sqlx::query(
            r#"
            SELECT a.id, a.mime_part_id
            FROM attachments a
            WHERE a.tenant_id = $1
              AND a.account_id = $2
              AND a.message_id = $3
              AND ($5 OR a.id = ANY($6))
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = a.tenant_id
                    AND mm.account_id = a.account_id
                    AND mm.id = $4
                    AND mm.message_id = a.message_id
                    AND mm.visibility = 'visible'
              )
            ORDER BY a.id
            FOR UPDATE OF a
            "#,
        )
        .bind(tenant_id)
        .bind(claim.account_id)
        .bind(claim.message_id)
        .bind(claim.membership_id)
        .bind(delete_all)
        .bind(requested_ids)
        .fetch_all(&mut **tx)
        .await?;
        if !delete_all && rows.len() != requested_ids.len() {
            bail!("submission source patch attachment does not belong to the claimed source");
        }
        if rows.is_empty() {
            return Ok(());
        }

        let deleted_attachment_ids = rows
            .iter()
            .map(|row| row.try_get::<Uuid, _>("id"))
            .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?;
        let deleted_mime_part_ids = rows
            .iter()
            .map(|row| row.try_get::<Option<Uuid>, _>("mime_part_id"))
            .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        sqlx::query(
            r#"
            DELETE FROM mapi_custom_property_values
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = 'attachment'
              AND canonical_id = ANY($3)
            "#,
        )
        .bind(tenant_id)
        .bind(claim.account_id)
        .bind(&deleted_attachment_ids)
        .execute(&mut **tx)
        .await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM attachments
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND id = ANY($4)
            "#,
        )
        .bind(tenant_id)
        .bind(claim.account_id)
        .bind(claim.message_id)
        .bind(&deleted_attachment_ids)
        .execute(&mut **tx)
        .await?;
        if deleted.rows_affected() != deleted_attachment_ids.len() as u64 {
            bail!("claimed submission source attachments changed during deletion");
        }
        if !deleted_mime_part_ids.is_empty() {
            sqlx::query(
                r#"
                UPDATE calendar_mail_classifications
                SET needs_reclassification = TRUE,
                    scheduling_mime_part_id = NULL,
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND message_id = $2
                  AND scheduling_mime_part_id = ANY($3)
                "#,
            )
            .bind(tenant_id)
            .bind(claim.message_id)
            .bind(&deleted_mime_part_ids)
            .execute(&mut **tx)
            .await?;
            sqlx::query(
                r#"
                DELETE FROM mime_parts
                WHERE tenant_id = $1
                  AND message_id = $2
                  AND id = ANY($3)
                "#,
            )
            .bind(tenant_id)
            .bind(claim.message_id)
            .bind(&deleted_mime_part_ids)
            .execute(&mut **tx)
            .await?;
        }
        sqlx::query(
            r#"
            UPDATE messages
            SET has_attachments = EXISTS (
                SELECT 1
                FROM attachments
                WHERE tenant_id = $1 AND message_id = $2
            )
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(claim.message_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(super) async fn copy_claimed_submission_source_custom_properties_to_sent_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        claim: &ClaimedSubmissionSource,
        sent_message_id: Uuid,
    ) -> Result<()> {
        if account_id != claim.account_id {
            bail!("claimed submission source account changed before Sent property copy");
        }
        sqlx::query(
            r#"
            INSERT INTO mapi_custom_property_values (
                tenant_id, account_id, object_kind, canonical_id,
                property_tag, property_type, property_value
            )
            SELECT
                value.tenant_id, value.account_id, 'message', $5,
                value.property_tag, value.property_type, value.property_value
            FROM mapi_custom_property_values value
            WHERE value.tenant_id = $1
              AND value.account_id = $2
              AND value.object_kind = 'message'
              AND value.canonical_id = $3
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = value.tenant_id
                    AND mm.account_id = value.account_id
                    AND mm.id = $4
                    AND mm.message_id = value.canonical_id
                    AND mm.visibility = 'visible'
              )
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(claim.message_id)
        .bind(claim.membership_id)
        .bind(sent_message_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(super) async fn copy_claimed_submission_source_followup_to_sent_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        claim: &ClaimedSubmissionSource,
        sent_mailbox_message_id: Uuid,
    ) -> Result<()> {
        if account_id != claim.account_id {
            bail!("claimed submission source account changed before Sent follow-up copy");
        }
        let copied = sqlx::query(
            r#"
            UPDATE mailbox_messages sent
            SET is_seen = source.is_seen,
                is_flagged = source.is_flagged,
                followup_flag_status = source.followup_flag_status,
                followup_icon = source.followup_icon,
                todo_item_flags = source.todo_item_flags,
                followup_request = source.followup_request,
                followup_start_at = source.followup_start_at,
                followup_due_at = source.followup_due_at,
                followup_completed_at = source.followup_completed_at,
                reminder_set = source.reminder_set,
                reminder_at = source.reminder_at,
                reminder_dismissed_at = source.reminder_dismissed_at,
                swapped_todo_store_id = source.swapped_todo_store_id,
                swapped_todo_data = source.swapped_todo_data,
                keywords = source.keywords,
                updated_at = NOW()
            FROM mailbox_messages source
            WHERE sent.tenant_id = $1
              AND sent.account_id = $2
              AND sent.id = $3
              AND sent.visibility = 'visible'
              AND source.tenant_id = sent.tenant_id
              AND source.account_id = sent.account_id
              AND source.id = $4
              AND source.message_id = $5
              AND source.visibility = 'visible'
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(sent_mailbox_message_id)
        .bind(claim.membership_id)
        .bind(claim.message_id)
        .execute(&mut **tx)
        .await?;
        if copied.rows_affected() != 1 {
            bail!("claimed submission source follow-up state was not copied to Sent");
        }
        Ok(())
    }
}
