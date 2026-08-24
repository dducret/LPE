use anyhow::{bail, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    mapi_message_identity::rotate_active_mapi_message_identity_in_tx, AuditEntryInput, Storage,
};

use super::{insert_visible_recipient, SubmittedRecipientInput};

impl Storage {
    pub async fn replace_message_recipients(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        to: &[SubmittedRecipientInput],
        cc: &[SubmittedRecipientInput],
        bcc: &[SubmittedRecipientInput],
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;
        let memberships = self
            .lock_visible_message_memberships_in_tx(&mut tx, &tenant_id, account_id, message_id)
            .await?;
        if memberships.is_empty() {
            bail!("message not found");
        }
        self.ensure_message_shared_state_is_private_to_account_in_tx(
            &mut tx, &tenant_id, account_id, message_id,
        )
        .await?;

        sqlx::query(
            r#"
            DELETE FROM message_recipients
            WHERE tenant_id = $1
              AND message_id = $2
              AND role IN ('to', 'cc')
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM protected_bcc_recipients WHERE tenant_id = $1 AND message_id = $2",
        )
        .bind(&tenant_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await?;

        for (ordinal, recipient) in to.iter().enumerate() {
            insert_visible_recipient(&mut tx, &tenant_id, message_id, "to", ordinal, recipient)
                .await?;
        }
        for (ordinal, recipient) in cc.iter().enumerate() {
            insert_visible_recipient(&mut tx, &tenant_id, message_id, "cc", ordinal, recipient)
                .await?;
        }
        for (ordinal, recipient) in bcc.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO protected_bcc_recipients (
                    id, tenant_id, message_id, owner_account_id, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(account_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            r#"
            UPDATE messages
            SET authorized_calendar_response_content_sha256 = NULL,
                calendar_response_processed = FALSE
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            UPDATE calendar_mail_classifications
            SET needs_reclassification = TRUE,
                scheduling_mime_part_id = NULL,
                updated_at = NOW()
            WHERE tenant_id = $1 AND message_id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await?;

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let rows = sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET modseq = $4, updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND visibility = 'visible'
            RETURNING id, mailbox_id, thread_id, imap_uid
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(modseq)
        .fetch_all(&mut *tx)
        .await?;
        if rows.len() != memberships.len() {
            bail!("visible message memberships changed while replacing recipients");
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
                    "recipientsChanged": true
                }),
            )
            .await?;
        }
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }
}
