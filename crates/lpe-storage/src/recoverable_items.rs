use anyhow::{anyhow, bail, Result};
use serde::Serialize;
use sqlx::Row;
use uuid::Uuid;

use crate::{AuditEntryInput, JmapEmail, Storage};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RecoverableItem {
    pub id: Uuid,
    pub message_id: Uuid,
    pub source_mailbox_message_id: Uuid,
    pub source_mailbox_id: Uuid,
    pub source_imap_uid: i64,
    pub recoverable_folder: String,
    pub delete_kind: String,
    pub status: String,
    pub deleted_at: String,
    pub retained_until: Option<String>,
    pub legal_hold: bool,
    pub subject: String,
    pub sender_address: String,
    pub received_at: String,
    pub size_octets: i64,
    pub has_attachments: bool,
}

impl Storage {
    pub async fn list_recoverable_items(
        &self,
        account_id: Uuid,
        recoverable_folder: Option<&str>,
    ) -> Result<Vec<RecoverableItem>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let folder = recoverable_folder
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(folder) = folder {
            if !matches!(folder, "deletions" | "versions" | "purges") {
                bail!("unsupported recoverable folder");
            }
        }

        let rows = sqlx::query(
            r#"
            SELECT
                ri.id,
                ri.message_id,
                ri.source_mailbox_message_id,
                ri.source_mailbox_id,
                ri.source_imap_uid,
                ri.recoverable_folder,
                ri.delete_kind,
                ri.status,
                to_char(ri.deleted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS deleted_at,
                CASE
                    WHEN ri.retained_until IS NULL THEN NULL
                    ELSE to_char(ri.retained_until AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS retained_until,
                ri.legal_hold,
                m.normalized_subject AS subject,
                COALESCE(sender.address, '') AS sender_address,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at,
                m.size_octets,
                m.has_attachments
            FROM recoverable_items ri
            JOIN messages m
              ON m.tenant_id = ri.tenant_id
             AND m.id = ri.message_id
            LEFT JOIN message_recipients sender
              ON sender.tenant_id = m.tenant_id
             AND sender.message_id = m.id
             AND sender.role = 'from'
             AND sender.ordinal = 0
            WHERE ri.tenant_id = $1
              AND ri.account_id = $2
              AND ri.status = 'active'
              AND ($3::text IS NULL OR ri.recoverable_folder = $3)
            ORDER BY ri.deleted_at DESC, ri.id DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(folder)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(RecoverableItem {
                    id: row.try_get("id")?,
                    message_id: row.try_get("message_id")?,
                    source_mailbox_message_id: row.try_get("source_mailbox_message_id")?,
                    source_mailbox_id: row.try_get("source_mailbox_id")?,
                    source_imap_uid: row.try_get("source_imap_uid")?,
                    recoverable_folder: row.try_get("recoverable_folder")?,
                    delete_kind: row.try_get("delete_kind")?,
                    status: row.try_get("status")?,
                    deleted_at: row.try_get("deleted_at")?,
                    retained_until: row.try_get("retained_until")?,
                    legal_hold: row.try_get("legal_hold")?,
                    subject: row.try_get("subject")?,
                    sender_address: row.try_get("sender_address")?,
                    received_at: row.try_get("received_at")?,
                    size_octets: row.try_get("size_octets")?,
                    has_attachments: row.try_get("has_attachments")?,
                })
            })
            .collect()
    }

    pub async fn restore_recoverable_item(
        &self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        target_mailbox_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let item = sqlx::query(
            r#"
            SELECT
                ri.message_id,
                ri.source_mailbox_message_id,
                ri.source_mailbox_id,
                ri.source_imap_uid,
                ri.recoverable_folder,
                ri.source_thread_id,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at
            FROM recoverable_items ri
            JOIN messages m
              ON m.tenant_id = ri.tenant_id
             AND m.id = ri.message_id
            WHERE ri.tenant_id = $1
              AND ri.account_id = $2
              AND ri.id = $3
              AND ri.status = 'active'
            FOR UPDATE OF ri
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(recoverable_item_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("recoverable item not found"))?;

        let message_id: Uuid = item.try_get("message_id")?;
        let source_mailbox_message_id: Uuid = item.try_get("source_mailbox_message_id")?;
        let source_imap_uid: i64 = item.try_get("source_imap_uid")?;
        let recoverable_folder: String = item.try_get("recoverable_folder")?;
        let target_mailbox_id = target_mailbox_id.unwrap_or(item.try_get("source_mailbox_id")?);
        let target_role = sqlx::query_scalar::<_, String>(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("target mailbox not found"))?;

        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mailbox_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mailbox_id = $3
                  AND message_id = $4
                  AND visibility <> 'expunged'
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?
        {
            bail!("message already exists in target mailbox");
        }

        let membership_id = self
            .allocate_mailbox_membership_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                target_mailbox_id,
                message_id,
                item.try_get("source_thread_id")?,
                &item.try_get::<String, _>("received_at")?,
                false,
                false,
                target_role == "drafts",
                "created",
            )
            .await?;

        self.rebuild_mail_search_document_in_tx(
            &mut tx,
            &tenant_id,
            account_id,
            membership_id,
            message_id,
        )
        .await?;

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "recoverable_item",
            recoverable_item_id,
            "moved",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "sourceMailboxMessageId": source_mailbox_message_id,
                "sourceImapUid": source_imap_uid,
                "recoverableFolder": recoverable_folder,
                "restoredMailboxMessageId": membership_id,
                "targetMailboxId": target_mailbox_id
            }),
        )
        .await?;

        sqlx::query(
            r#"
            UPDATE recoverable_items
            SET status = 'restored',
                restored_at = NOW(),
                restored_mailbox_message_id = $4,
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(recoverable_item_id)
        .bind(membership_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("restored message not found"))
    }

    pub async fn purge_recoverable_item(
        &self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let item = sqlx::query(
            r#"
            SELECT message_id, source_mailbox_message_id, recoverable_folder, legal_hold,
                   retained_until IS NOT NULL AND retained_until > NOW() AS retention_active
            FROM recoverable_items
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = $3
              AND status = 'active'
            FOR UPDATE
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(recoverable_item_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("recoverable item not found"))?;

        if item.try_get::<bool, _>("legal_hold")? {
            bail!("recoverable item legal hold is active");
        }
        if item.try_get::<bool, _>("retention_active")? {
            bail!("recoverable item retention is active");
        }

        let message_id: Uuid = item.try_get("message_id")?;
        let source_mailbox_message_id: Uuid = item.try_get("source_mailbox_message_id")?;
        let recoverable_folder: String = item.try_get("recoverable_folder")?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        let cursor = Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "recoverable_item",
            recoverable_item_id,
            "destroyed",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "sourceMailboxMessageId": source_mailbox_message_id,
                "recoverableFolder": recoverable_folder
            }),
        )
        .await?;

        sqlx::query(
            r#"
            INSERT INTO tombstones (
                id, tenant_id, account_id, object_kind, object_id,
                message_id, deleted_modseq, change_cursor, reason
            )
            VALUES ($1, $2, $3, 'recoverable_item', $4, $5, $6, $7, 'purge')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(recoverable_item_id)
        .bind(message_id)
        .bind(modseq)
        .bind(cursor)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE recoverable_items
            SET status = 'purged',
                purged_at = NOW(),
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(recoverable_item_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn rebuild_mail_search_document_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        mailbox_message_id: Uuid,
        message_id: Uuid,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO mail_search_documents (
                tenant_id, account_id, mailbox_message_id, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            )
            SELECT
                m.tenant_id,
                $2,
                $3,
                m.id,
                m.normalized_subject,
                COALESCE((
                    SELECT string_agg(COALESCE(NULLIF(display_name, ''), address), ' ')
                    FROM message_recipients
                    WHERE tenant_id = m.tenant_id
                      AND message_id = m.id
                      AND role IN ('from', 'sender', 'reply_to', 'to', 'cc')
                ), ''),
                COALESCE((
                    SELECT string_agg(body_text, ' ')
                    FROM message_bodies
                    WHERE tenant_id = m.tenant_id
                      AND message_id = m.id
                ), ''),
                '',
                to_tsvector(
                    'simple',
                    concat_ws(
                        ' ',
                        m.normalized_subject,
                        COALESCE((
                            SELECT string_agg(COALESCE(NULLIF(display_name, ''), address), ' ')
                            FROM message_recipients
                            WHERE tenant_id = m.tenant_id
                              AND message_id = m.id
                              AND role IN ('from', 'sender', 'reply_to', 'to', 'cc')
                        ), ''),
                        COALESCE((
                            SELECT string_agg(body_text, ' ')
                            FROM message_bodies
                            WHERE tenant_id = m.tenant_id
                              AND message_id = m.id
                        ), '')
                    )
                )
            FROM messages m
            WHERE m.tenant_id = $1 AND m.id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_message_id)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
