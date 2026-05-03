use anyhow::{anyhow, bail, Result};
use serde::Serialize;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    submission,
    submission::{AttachmentUploadInput, SubmittedRecipientInput},
    AccountQuotaRow, ActiveSyncAttachmentRow, ActiveSyncSyncStateRow, AuditEntryInput,
    ImapEmailRow, JmapEmailRecipientRow, JmapEmailRow, JmapEmailSubmissionRow, JmapMailboxRow,
    JmapUploadBlobRow, MessageBccRecipientRecordRow, Storage,
};

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncSyncState {
    pub sync_key: String,
    pub snapshot_json: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncItemState {
    pub id: Uuid,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncAttachment {
    pub id: Uuid,
    pub message_id: Uuid,
    pub file_name: String,
    pub media_type: String,
    pub size_octets: u64,
    pub file_reference: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncAttachmentContent {
    pub file_reference: String,
    pub file_name: String,
    pub media_type: String,
    pub blob_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapMailbox {
    pub id: Uuid,
    pub role: String,
    pub name: String,
    pub sort_order: i32,
    pub total_emails: u32,
    pub unread_emails: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailAddress {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmail {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub mailbox_id: Uuid,
    pub mailbox_role: String,
    pub mailbox_name: String,
    pub received_at: String,
    pub sent_at: Option<String>,
    pub from_address: String,
    pub from_display: Option<String>,
    pub sender_address: Option<String>,
    pub sender_display: Option<String>,
    pub sender_authorization_kind: String,
    pub submitted_by_account_id: Uuid,
    pub to: Vec<JmapEmailAddress>,
    pub cc: Vec<JmapEmailAddress>,
    pub bcc: Vec<JmapEmailAddress>,
    pub subject: String,
    pub preview: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub unread: bool,
    pub flagged: bool,
    pub has_attachments: bool,
    pub size_octets: i64,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: Option<String>,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImapEmail {
    pub id: Uuid,
    pub uid: u32,
    pub modseq: u64,
    pub thread_id: Uuid,
    pub mailbox_id: Uuid,
    pub mailbox_role: String,
    pub mailbox_name: String,
    pub received_at: String,
    pub sent_at: Option<String>,
    pub from_address: String,
    pub from_display: Option<String>,
    pub to: Vec<JmapEmailAddress>,
    pub cc: Vec<JmapEmailAddress>,
    pub bcc: Vec<JmapEmailAddress>,
    pub subject: String,
    pub preview: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub unread: bool,
    pub flagged: bool,
    pub has_attachments: bool,
    pub size_octets: i64,
    pub internet_message_id: Option<String>,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailQuery {
    pub ids: Vec<Uuid>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapThreadQuery {
    pub ids: Vec<Uuid>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailSubmission {
    pub id: Uuid,
    pub email_id: Uuid,
    pub thread_id: Uuid,
    pub identity_id: String,
    pub identity_email: String,
    pub envelope_mail_from: String,
    pub envelope_rcpt_to: Vec<String>,
    pub send_at: String,
    pub undo_status: String,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapQuota {
    pub id: String,
    pub name: String,
    pub used: u64,
    pub hard_limit: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapUploadBlob {
    pub id: Uuid,
    pub account_id: Uuid,
    pub media_type: String,
    pub octet_size: u64,
    pub blob_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct JmapMailboxCreateInput {
    pub account_id: Uuid,
    pub name: String,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct JmapMailboxUpdateInput {
    pub account_id: Uuid,
    pub mailbox_id: Uuid,
    pub name: Option<String>,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct JmapImportedEmailInput {
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub mailbox_id: Uuid,
    pub source: String,
    pub from_display: Option<String>,
    pub from_address: String,
    pub sender_display: Option<String>,
    pub sender_address: Option<String>,
    pub to: Vec<SubmittedRecipientInput>,
    pub cc: Vec<SubmittedRecipientInput>,
    pub bcc: Vec<SubmittedRecipientInput>,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: String,
    pub size_octets: i64,
    pub received_at: Option<String>,
    pub attachments: Vec<AttachmentUploadInput>,
}

impl Storage {
    pub async fn fetch_jmap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, JmapMailboxRow>(
            r#"
            SELECT
                mb.id,
                mb.role,
                mb.display_name,
                mb.sort_order,
                COUNT(m.id) AS total_emails,
                COUNT(*) FILTER (WHERE m.unread) AS unread_emails
            FROM mailboxes mb
            LEFT JOIN messages m
              ON m.mailbox_id = mb.id
             AND m.tenant_id = mb.tenant_id
             AND m.account_id = mb.account_id
            WHERE mb.tenant_id = $1
              AND mb.account_id = $2
            GROUP BY mb.id, mb.role, mb.display_name, mb.sort_order
            ORDER BY mb.sort_order ASC, lower(mb.display_name) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| JmapMailbox {
                id: row.id,
                role: row.role,
                name: row.display_name,
                sort_order: row.sort_order,
                total_emails: row.total_emails.max(0) as u32,
                unread_emails: row.unread_emails.max(0) as u32,
            })
            .collect())
    }

    pub async fn ensure_imap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "inbox", "Inbox", 0, 365)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "drafts", "Drafts", 10, 365)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "sent", "Sent", 20, 365)
            .await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(account_id).await
    }

    pub async fn fetch_imap_highest_modseq(&self, account_id: Uuid) -> Result<u64> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let modseq = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT mail_sync_modseq
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        u64::try_from(modseq).map_err(|_| anyhow!("mail sync modseq is out of range"))
    }

    pub async fn fetch_jmap_mailbox_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        Ok(self
            .fetch_jmap_mailboxes(account_id)
            .await?
            .into_iter()
            .map(|mailbox| mailbox.id)
            .collect())
    }

    pub async fn create_jmap_mailbox(
        &self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;

        let name = input.name.trim();
        if name.is_empty() {
            bail!("mailbox name is required");
        }

        let duplicate = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND lower(display_name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
        if duplicate.is_some() {
            bail!("mailbox already exists");
        }

        let next_sort_order = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT COALESCE(MAX(sort_order), 0) + 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .fetch_one(&mut *tx)
        .await?;

        let mailbox_id = Uuid::new_v4();
        let sort_order = input.sort_order.unwrap_or(next_sort_order);
        sqlx::query(
            r#"
            INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, sort_order, retention_days)
            VALUES ($1, $2, $3, 'custom', $4, $5, 365)
            "#,
        )
        .bind(mailbox_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(name)
        .bind(sort_order)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(input.account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == mailbox_id)
            .ok_or_else(|| anyhow!("mailbox creation failed"))
    }

    pub async fn update_jmap_mailbox(
        &self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let current = sqlx::query(
            r#"
            SELECT role, display_name, sort_order
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;

        let role = current.try_get::<String, _>("role")?;
        if is_system_mailbox_role(&role) {
            bail!("system mailbox cannot be modified through JMAP");
        }

        let current_name = current.try_get::<String, _>("display_name")?;
        let current_sort_order = current.try_get::<i32, _>("sort_order")?;
        let name = input
            .name
            .as_deref()
            .unwrap_or(&current_name)
            .trim()
            .to_string();
        if name.is_empty() {
            bail!("mailbox name is required");
        }

        let duplicate = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND lower(display_name) = lower($3) AND id <> $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(&name)
        .bind(input.mailbox_id)
        .fetch_optional(&mut *tx)
        .await?;
        if duplicate.is_some() {
            bail!("mailbox already exists");
        }

        sqlx::query(
            r#"
            UPDATE mailboxes
            SET display_name = $4, sort_order = $5
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .bind(name)
        .bind(input.sort_order.unwrap_or(current_sort_order))
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(input.account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == input.mailbox_id)
            .ok_or_else(|| anyhow!("mailbox update failed"))
    }

    pub async fn destroy_jmap_mailbox(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let current = sqlx::query(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;

        let role = current.try_get::<String, _>("role")?;
        if is_system_mailbox_role(&role) {
            bail!("system mailbox cannot be deleted through JMAP");
        }

        let message_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM messages
            WHERE tenant_id = $1 AND account_id = $2 AND mailbox_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_one(&mut *tx)
        .await?;
        if message_count > 0 {
            bail!("mailbox is not empty");
        }

        sqlx::query(
            r#"
            DELETE FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn query_jmap_email_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapEmailQuery> {
        let normalized_search = search_text
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let ids = sqlx::query(
            r#"
            SELECT s.message_id AS id
            FROM searchable_mail_documents s
            WHERE s.account_id = $1
              AND ($2::uuid IS NULL OR s.mailbox_id = $2)
              AND (
                $3::text IS NULL
                OR (s.message_search_vector || s.attachment_search_vector)
                    @@ websearch_to_tsquery('simple', $3)
              )
            ORDER BY s.received_at DESC, s.message_id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| row.try_get("id"))
        .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM searchable_mail_documents s
            WHERE s.account_id = $1
              AND ($2::uuid IS NULL OR s.mailbox_id = $2)
              AND (
                $3::text IS NULL
                OR (s.message_search_vector || s.attachment_search_vector)
                    @@ websearch_to_tsquery('simple', $3)
              )
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(JmapEmailQuery {
            ids,
            total: total.max(0) as u64,
        })
    }

    pub async fn fetch_all_jmap_email_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT id
            FROM messages
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY COALESCE(sent_at, received_at) DESC, id DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row.try_get("id").map_err(Into::into))
            .collect()
    }

    pub async fn fetch_all_jmap_thread_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT thread_id
            FROM messages
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY thread_id
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row.try_get("thread_id").map_err(Into::into))
            .collect()
    }

    pub async fn query_jmap_thread_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapThreadQuery> {
        let normalized_search = search_text
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let ids = sqlx::query(
            r#"
            WITH matched_threads AS (
                SELECT
                    m.thread_id,
                    MAX(s.received_at) AS latest_received_at
                FROM searchable_mail_documents s
                JOIN messages m ON m.id = s.message_id
                WHERE s.account_id = $1
                  AND ($2::uuid IS NULL OR s.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR (s.message_search_vector || s.attachment_search_vector)
                        @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY m.thread_id
            )
            SELECT thread_id
            FROM matched_threads
            ORDER BY latest_received_at DESC, thread_id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| row.try_get("thread_id"))
        .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM (
                SELECT m.thread_id
                FROM searchable_mail_documents s
                JOIN messages m ON m.id = s.message_id
                WHERE s.account_id = $1
                  AND ($2::uuid IS NULL OR s.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR (s.message_search_vector || s.attachment_search_vector)
                        @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY m.thread_id
            ) matched_threads
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(JmapThreadQuery {
            ids,
            total: total.max(0) as u64,
        })
    }

    pub async fn fetch_jmap_emails(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmail>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query_as::<_, JmapEmailRow>(
            r#"
            SELECT
                m.id,
                m.imap_modseq,
                m.thread_id,
                m.mailbox_id,
                mb.role AS mailbox_role,
                mb.display_name AS mailbox_name,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                m.from_address,
                NULLIF(m.from_display, '') AS from_display,
                NULLIF(m.sender_address, '') AS sender_address,
                NULLIF(m.sender_display, '') AS sender_display,
                m.sender_authorization_kind,
                m.submitted_by_account_id,
                m.subject_normalized AS subject,
                m.preview_text AS preview,
                COALESCE(b.body_text, '') AS body_text,
                b.body_html_sanitized,
                m.unread,
                m.flagged,
                m.has_attachments,
                m.size_octets,
                m.internet_message_id,
                m.mime_blob_ref,
                m.delivery_status
            FROM messages m
            JOIN mailboxes mb ON mb.id = m.mailbox_id
            LEFT JOIN message_bodies b ON b.message_id = m.id
            WHERE m.tenant_id = $1
              AND m.account_id = $2
              AND m.id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let recipient_rows = sqlx::query_as::<_, JmapEmailRecipientRow>(
            r#"
            SELECT
                r.message_id,
                r.kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            JOIN messages m ON m.id = r.message_id
            WHERE r.tenant_id = $1
              AND m.account_id = $2
              AND r.message_id = ANY($3)
            ORDER BY r.message_id ASC, r.kind ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let mut emails = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(row) = rows.iter().find(|row| row.id == *id) {
                let to = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == *id && recipient.kind == "to")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let cc = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == *id && recipient.kind == "cc")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let bcc = sqlx::query(
                    r#"
                    SELECT address, display_name
                    FROM message_bcc_recipients
                    WHERE tenant_id = $1 AND message_id = $2
                    ORDER BY ordinal ASC
                    "#,
                )
                .bind(&tenant_id)
                .bind(*id)
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|row| JmapEmailAddress {
                    address: row.try_get("address").unwrap_or_default(),
                    display_name: row.try_get("display_name").ok(),
                })
                .collect();

                emails.push(JmapEmail {
                    id: row.id,
                    thread_id: row.thread_id,
                    mailbox_id: row.mailbox_id,
                    mailbox_role: row.mailbox_role.clone(),
                    mailbox_name: row.mailbox_name.clone(),
                    received_at: row.received_at.clone(),
                    sent_at: row.sent_at.clone(),
                    from_address: row.from_address.clone(),
                    from_display: row.from_display.clone(),
                    sender_address: row.sender_address.clone(),
                    sender_display: row.sender_display.clone(),
                    sender_authorization_kind: row.sender_authorization_kind.clone(),
                    submitted_by_account_id: row.submitted_by_account_id,
                    to,
                    cc,
                    bcc,
                    subject: row.subject.clone(),
                    preview: row.preview.clone(),
                    body_text: row.body_text.clone(),
                    body_html_sanitized: row.body_html_sanitized.clone(),
                    unread: row.unread,
                    flagged: row.flagged,
                    has_attachments: row.has_attachments,
                    size_octets: row.size_octets,
                    internet_message_id: row.internet_message_id.clone(),
                    mime_blob_ref: row.mime_blob_ref.clone(),
                    delivery_status: row.delivery_status.clone(),
                });
            }
        }

        Ok(emails)
    }

    pub async fn fetch_imap_emails(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> Result<Vec<ImapEmail>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ImapEmailRow>(
            r#"
            SELECT
                m.id,
                m.imap_uid,
                m.imap_modseq,
                m.thread_id,
                m.mailbox_id,
                mb.role AS mailbox_role,
                mb.display_name AS mailbox_name,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                m.from_address,
                NULLIF(m.from_display, '') AS from_display,
                NULLIF(m.sender_address, '') AS sender_address,
                NULLIF(m.sender_display, '') AS sender_display,
                m.sender_authorization_kind,
                m.submitted_by_account_id,
                m.subject_normalized AS subject,
                m.preview_text AS preview,
                COALESCE(b.body_text, '') AS body_text,
                b.body_html_sanitized,
                m.unread,
                m.flagged,
                m.has_attachments,
                m.size_octets,
                m.internet_message_id,
                m.mime_blob_ref,
                m.delivery_status
            FROM messages m
            JOIN mailboxes mb ON mb.id = m.mailbox_id
            LEFT JOIN message_bodies b ON b.message_id = m.id
            WHERE m.tenant_id = $1
              AND m.account_id = $2
              AND m.mailbox_id = $3
            ORDER BY m.imap_uid ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_all(&self.pool)
        .await?;

        let message_ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        let recipient_rows = sqlx::query_as::<_, JmapEmailRecipientRow>(
            r#"
            SELECT
                r.message_id,
                r.kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            JOIN messages m ON m.id = r.message_id
            WHERE r.tenant_id = $1
              AND m.account_id = $2
              AND r.message_id = ANY($3)
            ORDER BY r.message_id ASC, r.kind ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&message_ids)
        .fetch_all(&self.pool)
        .await?;

        let bcc_rows = sqlx::query_as::<_, MessageBccRecipientRecordRow>(
            r#"
            SELECT message_id, address, display_name
            FROM message_bcc_recipients
            WHERE tenant_id = $1 AND message_id = ANY($2)
            ORDER BY message_id ASC, ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(&message_ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let uid = u32::try_from(row.imap_uid)
                    .map_err(|_| anyhow!("message IMAP UID is out of range"))?;
                let modseq = u64::try_from(row.imap_modseq)
                    .map_err(|_| anyhow!("message IMAP modseq is out of range"))?;
                let to = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.id && recipient.kind == "to")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let cc = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.id && recipient.kind == "cc")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let bcc = bcc_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.id)
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();

                Ok(ImapEmail {
                    id: row.id,
                    uid,
                    modseq,
                    thread_id: row.thread_id,
                    mailbox_id: row.mailbox_id,
                    mailbox_role: row.mailbox_role,
                    mailbox_name: row.mailbox_name,
                    received_at: row.received_at,
                    sent_at: row.sent_at,
                    from_address: row.from_address,
                    from_display: row.from_display,
                    to,
                    cc,
                    bcc,
                    subject: row.subject,
                    preview: row.preview,
                    body_text: row.body_text,
                    body_html_sanitized: row.body_html_sanitized,
                    unread: row.unread,
                    flagged: row.flagged,
                    has_attachments: row.has_attachments,
                    size_octets: row.size_octets,
                    internet_message_id: row.internet_message_id,
                    delivery_status: row.delivery_status,
                })
            })
            .collect()
    }

    pub async fn update_imap_flags(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_ids: &[Uuid],
        unread: Option<bool>,
        flagged: Option<bool>,
        unchanged_since: Option<u64>,
    ) -> Result<Vec<Uuid>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let unchanged_since_i64 = unchanged_since
            .map(i64::try_from)
            .transpose()
            .map_err(|_| anyhow!("UNCHANGEDSINCE is out of range"))?;
        let modified_ids = if let Some(limit) = unchanged_since_i64 {
            sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mailbox_id = $3
                  AND id = ANY($4)
                  AND imap_modseq > $5
                ORDER BY imap_uid ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(message_ids)
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?
        } else {
            Vec::new()
        };
        if modified_ids.len() == message_ids.len() {
            tx.rollback().await?;
            return Ok(modified_ids);
        }

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;

        sqlx::query(
            r#"
            UPDATE messages
            SET
                unread = COALESCE($4, unread),
                flagged = COALESCE($5, flagged),
                imap_modseq = $6
            WHERE tenant_id = $1
              AND account_id = $2
              AND mailbox_id = $3
              AND id = ANY($7)
              AND ($8::bigint IS NULL OR imap_modseq <= $8)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(unread)
        .bind(flagged)
        .bind(modseq)
        .bind(message_ids)
        .bind(unchanged_since_i64)
        .execute(&mut *tx)
        .await?;

        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        Ok(modified_ids)
    }

    pub async fn fetch_jmap_draft(&self, account_id: Uuid, id: Uuid) -> Result<Option<JmapEmail>> {
        let emails = self.fetch_jmap_emails(account_id, &[id]).await?;
        Ok(emails
            .into_iter()
            .find(|email| email.mailbox_role == "drafts"))
    }

    pub async fn fetch_jmap_email_submissions(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmailSubmission>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, JmapEmailSubmissionRow>(
            r#"
            SELECT
                q.id,
                q.message_id AS email_id,
                m.thread_id,
                m.from_address,
                NULLIF(m.sender_address, '') AS sender_address,
                m.sender_authorization_kind,
                to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS send_at,
                q.status AS queue_status,
                m.delivery_status
            FROM outbound_message_queue q
            JOIN messages m ON m.id = q.message_id
            WHERE q.tenant_id = $1
              AND q.account_id = $2
              AND ($3::uuid[] IS NULL OR q.id = ANY($3))
            ORDER BY q.created_at DESC, q.id DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(if ids.is_empty() {
            None::<Vec<Uuid>>
        } else {
            Some(ids.to_vec())
        })
        .fetch_all(&self.pool)
        .await?;

        let message_ids = rows.iter().map(|row| row.email_id).collect::<Vec<_>>();
        let recipient_rows = if message_ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as::<_, JmapEmailRecipientRow>(
                r#"
                SELECT
                    r.message_id,
                    r.kind,
                    r.address,
                    r.display_name,
                    r.ordinal AS _ordinal
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = ANY($2)
                ORDER BY r.message_id ASC, r.kind ASC, r.ordinal ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(&message_ids)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows
            .into_iter()
            .map(|row| JmapEmailSubmission {
                id: row.id,
                email_id: row.email_id,
                thread_id: row.thread_id,
                identity_id: submission::sender_identity_id(
                    submission::sender_authorization_kind_from_str(&row.sender_authorization_kind),
                    account_id,
                ),
                identity_email: row.from_address.clone(),
                envelope_mail_from: row
                    .sender_address
                    .clone()
                    .unwrap_or_else(|| row.from_address.clone()),
                envelope_rcpt_to: recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.email_id)
                    .map(|recipient| recipient.address.clone())
                    .collect(),
                send_at: row.send_at,
                undo_status: "final".to_string(),
                delivery_status: if row.delivery_status.trim().is_empty() {
                    row.queue_status
                } else {
                    row.delivery_status
                },
            })
            .collect())
    }

    pub async fn fetch_jmap_quota(&self, account_id: Uuid) -> Result<JmapQuota> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, AccountQuotaRow>(
            r#"
            SELECT quota_mb, used_mb
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        Ok(JmapQuota {
            id: "mail".to_string(),
            name: "Mail".to_string(),
            used: (row.used_mb.max(0) as u64) * 1024 * 1024,
            hard_limit: (row.quota_mb.max(0) as u64) * 1024 * 1024,
        })
    }

    pub async fn save_jmap_upload_blob(
        &self,
        account_id: Uuid,
        media_type: &str,
        blob_bytes: &[u8],
    ) -> Result<JmapUploadBlob> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;

        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO jmap_upload_blobs (id, tenant_id, account_id, media_type, octet_size, blob_bytes)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(id)
        .bind(&tenant_id)
        .bind(account_id)
        .bind(media_type.trim())
        .bind(blob_bytes.len() as i64)
        .bind(blob_bytes)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(JmapUploadBlob {
            id,
            account_id,
            media_type: media_type.trim().to_string(),
            octet_size: blob_bytes.len() as u64,
            blob_bytes: blob_bytes.to_vec(),
        })
    }

    pub async fn fetch_jmap_upload_blob(
        &self,
        account_id: Uuid,
        blob_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, JmapUploadBlobRow>(
            r#"
            SELECT id, account_id, media_type, octet_size, blob_bytes
            FROM jmap_upload_blobs
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(blob_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| JmapUploadBlob {
            id: row.id,
            account_id: row.account_id,
            media_type: row.media_type,
            octet_size: row.octet_size.max(0) as u64,
            blob_bytes: row.blob_bytes,
        }))
    }

    pub async fn store_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        sync_key: &str,
        snapshot_json: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        sqlx::query(
            r#"
            INSERT INTO activesync_sync_states (
                id, tenant_id, account_id, device_id, collection_id, sync_key, snapshot_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, account_id, device_id, collection_id, sync_key)
            DO UPDATE SET snapshot_json = EXCLUDED.snapshot_json
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_id.trim())
        .bind(sync_key.trim())
        .bind(snapshot_json.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        sync_key: &str,
    ) -> Result<Option<ActiveSyncSyncState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, ActiveSyncSyncStateRow>(
            r#"
            SELECT sync_key, snapshot_json
            FROM activesync_sync_states
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND collection_id = $4
              AND sync_key = $5
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_id.trim())
        .bind(sync_key.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ActiveSyncSyncState {
            sync_key: row.sync_key,
            snapshot_json: row.snapshot_json,
        }))
    }

    pub async fn fetch_activesync_email_states(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        position: u64,
        limit: u64,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                concat_ws(
                    '|',
                    m.subject_normalized,
                    m.preview_text,
                    COALESCE(b.content_hash, ''),
                    to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    CASE WHEN m.unread THEN '1' ELSE '0' END,
                    CASE WHEN m.flagged THEN '1' ELSE '0' END,
                    COALESCE(m.from_display, ''),
                    m.from_address,
                    COALESCE(recipients.to_recipients, ''),
                    COALESCE(recipients.cc_recipients, ''),
                    m.delivery_status
                ) AS fingerprint
            FROM messages m
            LEFT JOIN message_bodies b ON b.message_id = m.id
            LEFT JOIN LATERAL (
                SELECT
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.kind = 'to') AS to_recipients,
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.kind = 'cc') AS cc_recipients
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = m.id
            ) recipients ON TRUE
            WHERE m.tenant_id = $1
              AND m.account_id = $2
              AND m.mailbox_id = $3
            ORDER BY COALESCE(m.sent_at, m.received_at) DESC, m.id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_email_states_by_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                concat_ws(
                    '|',
                    m.subject_normalized,
                    m.preview_text,
                    COALESCE(b.content_hash, ''),
                    to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    CASE WHEN m.unread THEN '1' ELSE '0' END,
                    CASE WHEN m.flagged THEN '1' ELSE '0' END,
                    COALESCE(m.from_display, ''),
                    m.from_address,
                    COALESCE(recipients.to_recipients, ''),
                    COALESCE(recipients.cc_recipients, ''),
                    m.delivery_status
                ) AS fingerprint
            FROM messages m
            LEFT JOIN message_bodies b ON b.message_id = m.id
            LEFT JOIN LATERAL (
                SELECT
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.kind = 'to') AS to_recipients,
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.kind = 'cc') AS cc_recipients
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = m.id
            ) recipients ON TRUE
            WHERE m.tenant_id = $1
              AND m.account_id = $2
              AND m.mailbox_id = $3
              AND m.id = ANY($4)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_contact_states(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM contacts
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY name ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_contact_states_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM contacts
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_event_states(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM calendar_events
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY event_date ASC, event_time ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_event_states_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM calendar_events
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_message_attachments(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Vec<ActiveSyncAttachment>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ActiveSyncAttachmentRow>(
            r#"
            SELECT a.id, a.message_id, a.file_name, a.media_type, a.size_octets
            FROM attachments a
            JOIN messages m ON m.id = a.message_id
            WHERE a.tenant_id = $1
              AND m.account_id = $2
              AND a.message_id = $3
            ORDER BY a.file_name ASC, a.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ActiveSyncAttachment {
                id: row.id,
                message_id: row.message_id,
                file_name: row.file_name,
                media_type: row.media_type,
                size_octets: row.size_octets.max(0) as u64,
                file_reference: format!("attachment:{}:{}", row.message_id, row.id),
            })
            .collect())
    }

    pub async fn fetch_activesync_attachment_content(
        &self,
        account_id: Uuid,
        file_reference: &str,
    ) -> Result<Option<ActiveSyncAttachmentContent>> {
        let Some((message_id, attachment_id)) =
            crate::parse_activesync_file_reference(file_reference)
        else {
            return Ok(None);
        };
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let row = sqlx::query(
            r#"
            SELECT a.file_name, a.media_type, b.blob_bytes
            FROM attachments a
            JOIN messages m ON m.id = a.message_id
            JOIN attachment_blobs b ON b.id = a.attachment_blob_id
            WHERE a.tenant_id = $1
              AND a.id = $2
              AND a.message_id = $3
              AND m.account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(attachment_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ActiveSyncAttachmentContent {
            file_reference: file_reference.trim().to_string(),
            file_name: row.try_get("file_name").unwrap_or_default(),
            media_type: row.try_get("media_type").unwrap_or_default(),
            blob_bytes: row.try_get("blob_bytes").unwrap_or_default(),
        }))
    }
}

fn is_system_mailbox_role(role: &str) -> bool {
    let role = role.trim();
    !role.is_empty() && !role.eq_ignore_ascii_case("custom")
}

#[cfg(test)]
mod tests {
    use super::is_system_mailbox_role;

    #[test]
    fn custom_mailbox_role_is_user_managed() {
        assert!(!is_system_mailbox_role(""));
        assert!(!is_system_mailbox_role("custom"));
        assert!(!is_system_mailbox_role(" CUSTOM "));
        assert!(is_system_mailbox_role("inbox"));
        assert!(is_system_mailbox_role("sent"));
        assert!(is_system_mailbox_role("drafts"));
    }
}
