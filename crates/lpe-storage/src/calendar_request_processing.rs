use anyhow::{anyhow, bail, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    mapi_message_identity::rotate_active_mapi_message_identity_in_tx,
    protocols::CALENDAR_MAIL_PARSER_REVISION, AuditEntryInput, JmapEmail, Storage,
};

impl Storage {
    /// Records the attendee client's completed processing of one actionable
    /// Meeting Request. [MS-OXOCAL] sections 2.2.5.7 and 3.1.4.7.2 require
    /// PidTagProcessed to remain absent until that transition and then be TRUE.
    pub async fn mark_mapi_calendar_meeting_request_processed(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<(JmapEmail, bool)> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;

        let memberships = self
            .lock_visible_message_memberships_in_tx(&mut tx, &tenant_id, account_id, message_id)
            .await?;
        if memberships.is_empty() {
            bail!("meeting request message was not found");
        }

        let actionable = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM calendar_mail_classifications classification
                JOIN mime_parts scheduling_part
                  ON scheduling_part.tenant_id = classification.tenant_id
                 AND scheduling_part.message_id = classification.message_id
                 AND scheduling_part.id = classification.scheduling_mime_part_id
                 AND scheduling_part.is_scheduling_body
                 AND lower(btrim(split_part(scheduling_part.content_type, ';', 1))) = 'text/calendar'
                WHERE classification.tenant_id = $1
                  AND classification.message_id = $2
                  AND classification.parser_revision = $3
                  AND NOT classification.needs_reclassification
                  AND classification.classification = 'request'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM mailbox_messages visible_membership
                      LEFT JOIN calendar_mail_classification_projections projection
                        ON projection.tenant_id = visible_membership.tenant_id
                       AND projection.account_id = visible_membership.account_id
                       AND projection.message_id = visible_membership.message_id
                      WHERE visible_membership.tenant_id = classification.tenant_id
                        AND visible_membership.message_id = classification.message_id
                        AND visible_membership.visibility = 'visible'
                        AND projection.applied_generation IS DISTINCT FROM
                            classification.classification_generation
                  )
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(CALENDAR_MAIL_PARSER_REVISION)
        .fetch_one(&mut *tx)
        .await?;
        if !actionable {
            bail!("message is not a current actionable Meeting Request");
        }

        let already_processed = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT COALESCE(bool_and(calendar_request_processed), FALSE)
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND visibility = 'visible'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?;
        if already_processed {
            tx.commit().await?;
            let email = self
                .fetch_jmap_emails(account_id, &[message_id])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("processed meeting request was not found"))?;
            return Ok((email, false));
        }

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let rows = sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET calendar_request_processed = TRUE,
                modseq = $4,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND visibility = 'visible'
            RETURNING id, mailbox_id, COALESCE(thread_id, message_id) AS thread_id, imap_uid
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(modseq)
        .fetch_all(&mut *tx)
        .await?;
        if rows.len() != memberships.len() {
            bail!("visible meeting request memberships changed during processing");
        }

        rotate_active_mapi_message_identity_in_tx(&mut tx, &tenant_id, account_id, message_id)
            .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        for row in rows {
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(account_id),
                Some(row.try_get("mailbox_id")?),
                "mailbox_message",
                row.try_get("id")?,
                "updated",
                modseq,
                &principals,
                serde_json::json!({
                    "messageId": message_id,
                    "threadId": row.try_get::<Uuid, _>("thread_id")?,
                    "imapUid": row.try_get::<i64, _>("imap_uid")?,
                    "calendarRequestProcessedChanged": true
                }),
            )
            .await?;
        }
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        let email = self
            .fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("processed meeting request was not found"))?;
        Ok((email, true))
    }
}
