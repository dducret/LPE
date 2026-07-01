use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::Serialize;
use sqlx::Row;
use uuid::Uuid;

use crate::{ImapEmailRow, JmapEmailAddress, JmapEmailRecipientRow, Storage};

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
    pub deleted: bool,
    pub keywords: Vec<String>,
    pub has_attachments: bool,
    pub size_octets: i64,
    pub internet_message_id: Option<String>,
    pub delivery_status: String,
    pub mime_parts: Vec<ImapMimePart>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImapMimePart {
    pub part_path: String,
    pub content_type: String,
    pub content_disposition: Option<String>,
    pub content_id: Option<String>,
    pub file_name: Option<String>,
    pub transfer_encoding: Option<String>,
    pub charset_name: Option<String>,
    pub size_octets: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImapMailboxState {
    pub uid_validity: u32,
    pub uid_next: u32,
    pub highest_modseq: u64,
}

impl Storage {
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
                mm.imap_uid,
                mm.modseq AS imap_modseq,
                COALESCE(mm.thread_id, m.id) AS thread_id,
                mm.mailbox_id,
                mb.role AS mailbox_role,
                mb.display_name AS mailbox_name,
                to_char(mm.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                COALESCE(fr.address, '') AS from_address,
                NULLIF(fr.display_name, '') AS from_display,
                m.normalized_subject AS subject,
                LEFT(COALESCE(tb.body_text, hb.body_text, ''), 160) AS preview,
                COALESCE(tb.body_text, '') AS body_text,
                hb.sanitized_html AS body_html_sanitized,
                NOT mm.is_seen AS unread,
                mm.is_flagged AS flagged,
                mm.is_deleted AS imap_deleted,
                mm.keywords,
                m.has_attachments,
                m.size_octets,
                m.internet_message_id,
                COALESCE(sq.status, CASE WHEN mm.is_draft THEN 'draft' ELSE 'stored' END) AS delivery_status
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id AND fr.message_id = m.id AND fr.role = 'from'
            LEFT JOIN LATERAL (
                SELECT body_text
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'text'
                ORDER BY id ASC
                LIMIT 1
            ) tb ON TRUE
            LEFT JOIN LATERAL (
                SELECT body_text, sanitized_html
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'html'
                ORDER BY id ASC
                LIMIT 1
            ) hb ON TRUE
            LEFT JOIN submission_queue sq
              ON sq.tenant_id = mm.tenant_id
             AND sq.account_id = mm.account_id
             AND sq.sent_mailbox_message_id = mm.id
            WHERE m.tenant_id = $1
              AND mm.account_id = $2
              AND mm.mailbox_id = $3
              AND mm.visibility = 'visible'
            ORDER BY mm.imap_uid ASC
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
                r.role AS kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            WHERE r.tenant_id = $1
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = r.tenant_id
                    AND mm.account_id = $2
                    AND mm.message_id = r.message_id
                    AND mm.visibility = 'visible'
              )
              AND r.message_id = ANY($3)
            ORDER BY r.message_id ASC, r.role ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&message_ids)
        .fetch_all(&self.pool)
        .await?;

        let part_rows = sqlx::query(
            r#"
            SELECT
                mp.message_id,
                mp.part_path,
                mp.content_type,
                mp.content_disposition,
                mp.content_id,
                mp.file_name,
                mp.transfer_encoding,
                mp.charset_name,
                mp.size_octets
            FROM mime_parts mp
            WHERE mp.tenant_id = $1
              AND mp.message_id = ANY($2)
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = mp.tenant_id
                    AND mm.account_id = $3
                    AND mm.message_id = mp.message_id
                    AND mm.visibility = 'visible'
              )
            ORDER BY mp.message_id ASC, mp.ordinal ASC, mp.part_path ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(&message_ids)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        let mut mime_parts_by_message: HashMap<Uuid, Vec<ImapMimePart>> = HashMap::new();
        for part in part_rows {
            let message_id: Uuid = part.try_get("message_id")?;
            mime_parts_by_message
                .entry(message_id)
                .or_default()
                .push(ImapMimePart {
                    part_path: part.try_get("part_path")?,
                    content_type: part.try_get("content_type")?,
                    content_disposition: part.try_get("content_disposition")?,
                    content_id: part.try_get("content_id")?,
                    file_name: part.try_get("file_name")?,
                    transfer_encoding: part.try_get("transfer_encoding")?,
                    charset_name: part.try_get("charset_name")?,
                    size_octets: part.try_get("size_octets")?,
                });
        }

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
                    bcc: Vec::new(),
                    subject: row.subject,
                    preview: row.preview,
                    body_text: row.body_text,
                    body_html_sanitized: row.body_html_sanitized,
                    unread: row.unread,
                    flagged: row.flagged,
                    deleted: row.imap_deleted,
                    keywords: row.keywords,
                    has_attachments: row.has_attachments,
                    size_octets: row.size_octets,
                    internet_message_id: row.internet_message_id,
                    delivery_status: row.delivery_status,
                    mime_parts: mime_parts_by_message.remove(&row.id).unwrap_or_default(),
                })
            })
            .collect()
    }
}
