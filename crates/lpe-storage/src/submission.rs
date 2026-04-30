use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    normalize_email, normalize_subject, preview_text, trim_optional_text, AuditEntryInput,
    JmapEmailRecipientRow, MailboxAccountAccessRow, MailboxDelegationGrantRow,
    SenderDelegationGrantRow, Storage,
};

#[derive(Debug, Clone)]
pub struct SubmitMessageInput {
    pub draft_message_id: Option<Uuid>,
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
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
    pub mime_blob_ref: Option<String>,
    pub size_octets: i64,
    pub unread: Option<bool>,
    pub flagged: Option<bool>,
    pub attachments: Vec<AttachmentUploadInput>,
}

#[derive(Debug, Clone)]
pub struct SubmittedRecipientInput {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AttachmentUploadInput {
    pub file_name: String,
    pub media_type: String,
    pub blob_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SubmissionAccountIdentity {
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubmittedMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub sent_mailbox_id: Uuid,
    pub outbound_queue_id: Uuid,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SavedDraftMessage {
    pub message_id: Uuid,
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub draft_mailbox_id: Uuid,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalSubmissionPhase {
    EnsureSentMailbox,
    PersistSentMessage,
    PersistOutboundQueue,
    DeleteSourceDraft,
}

fn canonical_submission_phases(has_source_draft: bool) -> Vec<CanonicalSubmissionPhase> {
    let mut phases = vec![
        CanonicalSubmissionPhase::EnsureSentMailbox,
        CanonicalSubmissionPhase::PersistSentMessage,
        CanonicalSubmissionPhase::PersistOutboundQueue,
    ];
    if has_source_draft {
        phases.push(CanonicalSubmissionPhase::DeleteSourceDraft);
    }
    phases
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SenderAuthorizationKind {
    SelfSend,
    SendAs,
    SendOnBehalf,
}

impl SenderAuthorizationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SelfSend => "self",
            Self::SendAs => "send-as",
            Self::SendOnBehalf => "send-on-behalf",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SenderDelegationRight {
    SendAs,
    SendOnBehalf,
}

impl SenderDelegationRight {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SendAs => "send_as",
            Self::SendOnBehalf => "send_on_behalf",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxAccountAccess {
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub is_owned: bool,
    pub may_read: bool,
    pub may_write: bool,
    pub may_send_as: bool,
    pub may_send_on_behalf: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderIdentity {
    pub id: String,
    pub owner_account_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub authorization_kind: String,
    pub sender_address: Option<String>,
    pub sender_display: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MailboxDelegationGrantInput {
    pub owner_account_id: Uuid,
    pub grantee_email: String,
}

#[derive(Debug, Clone)]
pub struct SenderDelegationGrantInput {
    pub owner_account_id: Uuid,
    pub grantee_email: String,
    pub sender_right: SenderDelegationRight,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxDelegationGrant {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderDelegationGrant {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub sender_right: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxDelegationOverview {
    pub outgoing_mailboxes: Vec<MailboxDelegationGrant>,
    pub incoming_mailboxes: Vec<MailboxAccountAccess>,
    pub outgoing_sender_rights: Vec<SenderDelegationGrant>,
}

#[derive(Debug, Clone)]
pub(crate) struct AccountIdentity {
    pub(crate) id: Uuid,
    pub(crate) email: String,
    pub(crate) display_name: String,
}

#[derive(Debug)]
struct ResolvedSubmissionAuthorization {
    submitted_by: AccountIdentity,
    from_address: String,
    from_display: Option<String>,
    sender_address: Option<String>,
    sender_display: Option<String>,
    authorization_kind: SenderAuthorizationKind,
}

impl Storage {
    pub async fn save_draft_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SavedDraftMessage> {
        let from_address = normalize_email(&input.from_address);
        let subject = normalize_subject(&input.subject);
        let body_text = input.body_text.trim().to_string();
        let visible_recipients = normalize_visible_recipients(&input);
        let bcc_recipients = normalize_bcc_recipients(&input);

        if from_address.is_empty() {
            bail!("from_address is required");
        }

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        self.ensure_same_tenant_account_in_tx(&mut tx, &tenant_id, input.submitted_by_account_id)
            .await?;
        let authorization = self
            .resolve_submission_authorization_in_tx(&mut tx, &tenant_id, &input)
            .await?;
        let draft_mailbox_id = self
            .ensure_mailbox(
                &mut tx,
                &tenant_id,
                input.account_id,
                "drafts",
                "Drafts",
                10,
                365,
            )
            .await?;

        let message_id = input.draft_message_id.unwrap_or_else(Uuid::new_v4);
        let thread_id = Uuid::new_v4();
        let preview_text = preview_text(&body_text);
        let participants_normalized = participants_normalized(&from_address, &visible_recipients);
        let mime_blob_ref = input
            .mime_blob_ref
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("draft-message:{message_id}"));
        let content_hash = format!("draft:{message_id}");
        let unread = input.unread.unwrap_or(false);
        let flagged = input.flagged.unwrap_or(false);
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;

        if input.draft_message_id.is_some() {
            let updated = sqlx::query(
                r#"
                UPDATE messages m
                SET
                    internet_message_id = $5,
                    received_at = NOW(),
                    sent_at = NULL,
                    from_display = $6,
                    from_address = $7,
                    sender_display = $8,
                    sender_address = $9,
                    sender_authorization_kind = $10,
                    submitted_by_account_id = $11,
                    subject_normalized = $12,
                    preview_text = $13,
                    size_octets = $14,
                    mime_blob_ref = $15,
                    unread = $16,
                    flagged = $17,
                    has_attachments = FALSE,
                    submission_source = $18,
                    imap_modseq = $19,
                    delivery_status = 'draft'
                FROM mailboxes mb
                WHERE m.mailbox_id = mb.id
                  AND m.tenant_id = $1
                  AND m.account_id = $2
                  AND m.id = $3
                  AND mb.role = 'drafts'
                  AND mb.id = $4
                "#,
            )
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(message_id)
            .bind(draft_mailbox_id)
            .bind(input.internet_message_id)
            .bind(authorization.from_display.as_deref())
            .bind(&authorization.from_address)
            .bind(authorization.sender_display.as_deref())
            .bind(authorization.sender_address.as_deref())
            .bind(authorization.authorization_kind.as_str())
            .bind(authorization.submitted_by.id)
            .bind(&subject)
            .bind(&preview_text)
            .bind(input.size_octets.max(0))
            .bind(&mime_blob_ref)
            .bind(unread)
            .bind(flagged)
            .bind(input.source.trim().to_lowercase())
            .bind(modseq)
            .execute(&mut *tx)
            .await?;

            if updated.rows_affected() == 0 {
                bail!("draft not found");
            }

            sqlx::query("DELETE FROM message_recipients WHERE tenant_id = $1 AND message_id = $2")
                .bind(&tenant_id)
                .bind(message_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "DELETE FROM message_bcc_recipients WHERE tenant_id = $1 AND message_id = $2",
            )
            .bind(&tenant_id)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM attachments WHERE tenant_id = $1 AND message_id = $2")
                .bind(&tenant_id)
                .bind(message_id)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(
                r#"
                INSERT INTO messages (
                    id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                    imap_modseq, received_at, sent_at, from_display, from_address, sender_display,
                    sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                    preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                    submission_source, delivery_status
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, NOW(), NULL, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, FALSE, $18, $19,
                    $20, 'draft'
                )
                "#,
            )
            .bind(message_id)
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(draft_mailbox_id)
            .bind(thread_id)
            .bind(input.internet_message_id)
            .bind(modseq)
            .bind(authorization.from_display.as_deref())
            .bind(&authorization.from_address)
            .bind(authorization.sender_display.as_deref())
            .bind(authorization.sender_address.as_deref())
            .bind(authorization.authorization_kind.as_str())
            .bind(authorization.submitted_by.id)
            .bind(&subject)
            .bind(&preview_text)
            .bind(unread)
            .bind(flagged)
            .bind(input.size_octets.max(0))
            .bind(&mime_blob_ref)
            .bind(input.source.trim().to_lowercase())
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, $3, $4, NULL, $5, to_tsvector('simple', $6))
            ON CONFLICT (message_id) DO UPDATE SET
                body_text = EXCLUDED.body_text,
                body_html_sanitized = EXCLUDED.body_html_sanitized,
                participants_normalized = EXCLUDED.participants_normalized,
                content_hash = EXCLUDED.content_hash,
                search_vector = EXCLUDED.search_vector
            "#,
        )
        .bind(message_id)
        .bind(&body_text)
        .bind(input.body_html_sanitized)
        .bind(&participants_normalized)
        .bind(content_hash)
        .bind(format!("{subject} {body_text} {participants_normalized}"))
        .execute(&mut *tx)
        .await?;

        for (ordinal, (kind, recipient)) in visible_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (
                    id, tenant_id, message_id, kind, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(kind)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        for (ordinal, recipient) in bcc_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_bcc_recipients (
                    id, tenant_id, message_id, address, display_name, ordinal, metadata_scope
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'audit-compliance')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        self.ingest_message_attachments_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            message_id,
            &input.attachments,
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        Ok(SavedDraftMessage {
            message_id,
            account_id: input.account_id,
            submitted_by_account_id: input.submitted_by_account_id,
            draft_mailbox_id,
            delivery_status: "draft".to_string(),
        })
    }

    pub async fn fetch_account_identity(&self, account_id: Uuid) -> Result<MailboxAccountAccess> {
        let account = self.account_identity_for_id(account_id).await?;
        Ok(MailboxAccountAccess {
            account_id: account.id,
            email: account.email,
            display_name: account.display_name,
            is_owned: true,
            may_read: true,
            may_write: true,
            may_send_as: true,
            may_send_on_behalf: false,
        })
    }

    pub async fn upsert_mailbox_delegation_grant(
        &self,
        input: MailboxDelegationGrantInput,
        audit: AuditEntryInput,
    ) -> Result<MailboxDelegationGrant> {
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let grantee_email = normalize_email(&input.grantee_email);
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        sqlx::query(
            r#"
            INSERT INTO mailbox_delegation_grants (
                id, tenant_id, owner_account_id, grantee_account_id
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tenant_id, owner_account_id, grantee_account_id)
            DO UPDATE SET updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(owner.id)
        .bind(grantee.id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(&mut tx, &tenant_id, owner.id, grantee.id).await?;
        tx.commit().await?;

        self.fetch_mailbox_delegation_grant(owner.id, grantee.id)
            .await?
            .ok_or_else(|| anyhow!("mailbox delegation grant not found after upsert"))
    }

    pub async fn delete_mailbox_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM mailbox_delegation_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND grantee_account_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("mailbox delegation grant not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(
            &mut tx,
            &tenant_id,
            owner_account_id,
            grantee_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_mailbox_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<Option<MailboxDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, MailboxDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM mailbox_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.grantee_account_id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_mailbox_delegation_grant))
    }

    pub async fn upsert_sender_delegation_grant(
        &self,
        input: SenderDelegationGrantInput,
        audit: AuditEntryInput,
    ) -> Result<SenderDelegationGrant> {
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let grantee_email = normalize_email(&input.grantee_email);
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        sqlx::query(
            r#"
            INSERT INTO sender_delegation_grants (
                id, tenant_id, owner_account_id, grantee_account_id, sender_right
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_id, owner_account_id, grantee_account_id, sender_right)
            DO UPDATE SET updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(owner.id)
        .bind(grantee.id)
        .bind(input.sender_right.as_str())
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(&mut tx, &tenant_id, owner.id, grantee.id).await?;
        tx.commit().await?;

        self.fetch_sender_delegation_grant(owner.id, grantee.id, input.sender_right)
            .await?
            .ok_or_else(|| anyhow!("sender delegation grant not found after upsert"))
    }

    pub async fn delete_sender_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM sender_delegation_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND grantee_account_id = $3
              AND sender_right = $4
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .bind(sender_right.as_str())
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("sender delegation grant not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(
            &mut tx,
            &tenant_id,
            owner_account_id,
            grantee_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_sender_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
    ) -> Result<Option<SenderDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, SenderDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.sender_right,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sender_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.grantee_account_id = $3
              AND g.sender_right = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .bind(sender_right.as_str())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_sender_delegation_grant))
    }

    pub async fn fetch_outgoing_mailbox_delegation_grants(
        &self,
        owner_account_id: Uuid,
    ) -> Result<Vec<MailboxDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, MailboxDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM mailbox_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
            ORDER BY lower(grantee.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_mailbox_delegation_grant).collect())
    }

    pub async fn fetch_outgoing_sender_delegation_grants(
        &self,
        owner_account_id: Uuid,
    ) -> Result<Vec<SenderDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, SenderDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.sender_right,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sender_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
            ORDER BY lower(grantee.primary_email) ASC, g.sender_right ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_sender_delegation_grant).collect())
    }

    pub async fn fetch_accessible_mailbox_accounts(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<MailboxAccountAccess>> {
        let principal = self.account_identity_for_id(principal_account_id).await?;
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let mut accounts = vec![MailboxAccountAccess {
            account_id: principal.id,
            email: principal.email,
            display_name: principal.display_name,
            is_owned: true,
            may_read: true,
            may_write: true,
            may_send_as: true,
            may_send_on_behalf: false,
        }];

        let rows = sqlx::query_as::<_, MailboxAccountAccessRow>(
            r#"
            SELECT
                owner.id AS account_id,
                owner.primary_email AS email,
                owner.display_name,
                EXISTS(
                    SELECT 1
                    FROM sender_delegation_grants sg
                    WHERE sg.tenant_id = g.tenant_id
                      AND sg.owner_account_id = g.owner_account_id
                      AND sg.grantee_account_id = g.grantee_account_id
                      AND sg.sender_right = 'send_as'
                ) AS may_send_as,
                EXISTS(
                    SELECT 1
                    FROM sender_delegation_grants sg
                    WHERE sg.tenant_id = g.tenant_id
                      AND sg.owner_account_id = g.owner_account_id
                      AND sg.grantee_account_id = g.grantee_account_id
                      AND sg.sender_right = 'send_on_behalf'
                ) AS may_send_on_behalf
            FROM mailbox_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            WHERE g.tenant_id = $1
              AND g.grantee_account_id = $2
            ORDER BY lower(owner.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .fetch_all(&self.pool)
        .await?;

        accounts.extend(rows.into_iter().map(|row| MailboxAccountAccess {
            account_id: row.account_id,
            email: row.email,
            display_name: row.display_name,
            is_owned: false,
            may_read: true,
            may_write: true,
            may_send_as: row.may_send_as,
            may_send_on_behalf: row.may_send_on_behalf,
        }));
        Ok(accounts)
    }

    pub async fn require_mailbox_account_access(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<MailboxAccountAccess> {
        self.fetch_accessible_mailbox_accounts(principal_account_id)
            .await?
            .into_iter()
            .find(|account| account.account_id == target_account_id)
            .ok_or_else(|| anyhow!("mailbox account is not accessible"))
    }

    pub async fn fetch_sender_identities(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<Vec<SenderIdentity>> {
        let access = self
            .require_mailbox_account_access(principal_account_id, target_account_id)
            .await?;
        let principal = self.account_identity_for_id(principal_account_id).await?;

        let mut identities = Vec::new();
        if access.is_owned {
            identities.push(SenderIdentity {
                id: sender_identity_id(SenderAuthorizationKind::SelfSend, target_account_id),
                owner_account_id: target_account_id,
                email: access.email.clone(),
                display_name: access.display_name.clone(),
                authorization_kind: SenderAuthorizationKind::SelfSend.as_str().to_string(),
                sender_address: None,
                sender_display: None,
            });
        } else {
            if access.may_send_as {
                identities.push(SenderIdentity {
                    id: sender_identity_id(SenderAuthorizationKind::SendAs, target_account_id),
                    owner_account_id: target_account_id,
                    email: access.email.clone(),
                    display_name: access.display_name.clone(),
                    authorization_kind: SenderAuthorizationKind::SendAs.as_str().to_string(),
                    sender_address: None,
                    sender_display: None,
                });
            }
            if access.may_send_on_behalf {
                identities.push(SenderIdentity {
                    id: sender_identity_id(
                        SenderAuthorizationKind::SendOnBehalf,
                        target_account_id,
                    ),
                    owner_account_id: target_account_id,
                    email: access.email,
                    display_name: access.display_name,
                    authorization_kind: SenderAuthorizationKind::SendOnBehalf.as_str().to_string(),
                    sender_address: Some(principal.email),
                    sender_display: Some(principal.display_name),
                });
            }
        }

        Ok(identities)
    }

    pub async fn submit_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        let subject = normalize_subject(&input.subject);
        let body_text = input.body_text.trim().to_string();
        let visible_recipients = normalize_visible_recipients(&input);
        let bcc_recipients = normalize_bcc_recipients(&input);

        if visible_recipients.is_empty() && bcc_recipients.is_empty() {
            bail!("at least one recipient is required");
        }
        if subject.is_empty() && body_text.is_empty() {
            bail!("subject or body_text is required");
        }

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;

        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .fetch_optional(&mut *tx)
        .await?;

        if account_exists.is_none() {
            bail!("account not found");
        }

        let authorization = self
            .resolve_submission_authorization_in_tx(&mut tx, &tenant_id, &input)
            .await?;

        let message_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let outbound_queue_id = Uuid::new_v4();
        let preview_text = preview_text(&body_text);
        let participants_normalized =
            participants_normalized(&authorization.from_address, &visible_recipients);
        let mime_blob_ref = input
            .mime_blob_ref
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("canonical-message:{message_id}"));
        let content_hash = format!("message:{message_id}");
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        let mut sent_mailbox_id = None;

        for phase in canonical_submission_phases(input.draft_message_id.is_some()) {
            match phase {
                CanonicalSubmissionPhase::EnsureSentMailbox => {
                    sent_mailbox_id = Some(
                        self.ensure_mailbox(
                            &mut tx,
                            &tenant_id,
                            input.account_id,
                            "sent",
                            "Sent",
                            20,
                            365,
                        )
                        .await?,
                    );
                }
                CanonicalSubmissionPhase::PersistSentMessage => {
                    let sent_mailbox_id = sent_mailbox_id
                        .ok_or_else(|| anyhow!("sent mailbox must exist before submission"))?;
                    sqlx::query(
                        r#"
                        INSERT INTO messages (
                            id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                            imap_modseq, received_at, sent_at, from_display, from_address, sender_display,
                            sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                            preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                            submission_source, delivery_status
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6,
                            $7, NOW(), NOW(), $8, $9, $10,
                            $11, $12, $13, $14, FALSE, FALSE, FALSE, $15, $16,
                            $17, 'queued'
                        )
                        "#,
                    )
                    .bind(message_id)
                    .bind(&tenant_id)
                    .bind(input.account_id)
                    .bind(sent_mailbox_id)
                    .bind(thread_id)
                    .bind(input.internet_message_id.clone())
                    .bind(modseq)
                    .bind(authorization.from_display.as_deref())
                    .bind(&authorization.from_address)
                    .bind(authorization.sender_display.as_deref())
                    .bind(authorization.sender_address.as_deref())
                    .bind(authorization.authorization_kind.as_str())
                    .bind(authorization.submitted_by.id)
                    .bind(&subject)
                    .bind(&preview_text)
                    .bind(input.size_octets.max(0))
                    .bind(&mime_blob_ref)
                    .bind(input.source.trim().to_lowercase())
                    .execute(&mut *tx)
                    .await?;

                    sqlx::query(
                        r#"
                        INSERT INTO message_bodies (
                            message_id, body_text, body_html_sanitized, participants_normalized,
                            language_code, content_hash, search_vector
                        )
                        VALUES ($1, $2, $3, $4, NULL, $5, to_tsvector('simple', $6))
                        "#,
                    )
                    .bind(message_id)
                    .bind(&body_text)
                    .bind(input.body_html_sanitized.clone())
                    .bind(&participants_normalized)
                    .bind(content_hash.clone())
                    .bind(format!("{subject} {body_text} {participants_normalized}"))
                    .execute(&mut *tx)
                    .await?;

                    for (ordinal, (kind, recipient)) in visible_recipients.iter().enumerate() {
                        sqlx::query(
                            r#"
                            INSERT INTO message_recipients (
                                id, tenant_id, message_id, kind, address, display_name, ordinal
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            "#,
                        )
                        .bind(Uuid::new_v4())
                        .bind(&tenant_id)
                        .bind(message_id)
                        .bind(kind)
                        .bind(&recipient.address)
                        .bind(recipient.display_name.as_deref())
                        .bind(ordinal as i32)
                        .execute(&mut *tx)
                        .await?;
                    }

                    for (ordinal, recipient) in bcc_recipients.iter().enumerate() {
                        sqlx::query(
                            r#"
                            INSERT INTO message_bcc_recipients (
                                id, tenant_id, message_id, address, display_name, ordinal, metadata_scope
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, 'audit-compliance')
                            "#,
                        )
                        .bind(Uuid::new_v4())
                        .bind(&tenant_id)
                        .bind(message_id)
                        .bind(&recipient.address)
                        .bind(recipient.display_name.as_deref())
                        .bind(ordinal as i32)
                        .execute(&mut *tx)
                        .await?;
                    }

                    self.ingest_message_attachments_in_tx(
                        &mut tx,
                        &tenant_id,
                        input.account_id,
                        message_id,
                        &input.attachments,
                    )
                    .await?;
                }
                CanonicalSubmissionPhase::PersistOutboundQueue => {
                    sqlx::query(
                        r#"
                        INSERT INTO outbound_message_queue (
                            id, tenant_id, message_id, account_id, transport, status
                        )
                        VALUES ($1, $2, $3, $4, 'lpe-ct-smtp', 'queued')
                        "#,
                    )
                    .bind(outbound_queue_id)
                    .bind(&tenant_id)
                    .bind(message_id)
                    .bind(input.account_id)
                    .execute(&mut *tx)
                    .await?;
                }
                CanonicalSubmissionPhase::DeleteSourceDraft => {
                    if let Some(draft_message_id) = input.draft_message_id {
                        self.delete_draft_message_in_tx(
                            &mut tx,
                            &tenant_id,
                            input.account_id,
                            draft_message_id,
                        )
                        .await?;
                    }
                }
            }
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        let sent_mailbox_id =
            sent_mailbox_id.ok_or_else(|| anyhow!("sent mailbox must exist after submission"))?;
        Ok(SubmittedMessage {
            message_id,
            thread_id,
            account_id: input.account_id,
            submitted_by_account_id: authorization.submitted_by.id,
            sent_mailbox_id,
            outbound_queue_id,
            delivery_status: "queued".to_string(),
        })
    }

    pub async fn submit_draft_message(
        &self,
        account_id: Uuid,
        draft_message_id: Uuid,
        source: &str,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let draft = self
            .fetch_jmap_draft(account_id, draft_message_id)
            .await?
            .ok_or_else(|| anyhow!("draft not found"))?;

        let recipient_rows = sqlx::query_as::<_, JmapEmailRecipientRow>(
            r#"
            SELECT
                r.message_id,
                r.kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            WHERE r.tenant_id = $1
              AND r.message_id = $2
            ORDER BY r.kind ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(draft_message_id)
        .fetch_all(&self.pool)
        .await?;

        let bcc_rows = sqlx::query(
            r#"
            SELECT address, display_name
            FROM message_bcc_recipients
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(draft_message_id)
        .fetch_all(&self.pool)
        .await?;

        let to = recipient_rows
            .iter()
            .filter(|recipient| recipient.kind == "to")
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
        let cc = recipient_rows
            .iter()
            .filter(|recipient| recipient.kind == "cc")
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
        let bcc = bcc_rows
            .into_iter()
            .map(|row| SubmittedRecipientInput {
                address: row.try_get("address").unwrap_or_default(),
                display_name: row.try_get("display_name").ok(),
            })
            .collect();

        self.submit_message(
            SubmitMessageInput {
                draft_message_id: Some(draft_message_id),
                account_id,
                submitted_by_account_id: draft.submitted_by_account_id,
                source: source.trim().to_lowercase(),
                from_display: draft.from_display,
                from_address: draft.from_address,
                sender_display: draft.sender_display,
                sender_address: draft.sender_address,
                to,
                cc,
                bcc,
                subject: draft.subject,
                body_text: draft.body_text,
                body_html_sanitized: draft.body_html_sanitized,
                internet_message_id: draft.internet_message_id,
                mime_blob_ref: None,
                size_octets: draft.size_octets,
                unread: Some(draft.unread),
                flagged: Some(draft.flagged),
                attachments: Vec::new(),
            },
            audit,
        )
        .await
    }

    pub async fn delete_draft_message(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.delete_draft_message_in_tx(&mut tx, &tenant_id, account_id, message_id)
            .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn account_identity_for_id(
        &self,
        account_id: Uuid,
    ) -> Result<AccountIdentity> {
        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        Ok(AccountIdentity {
            id: row.try_get("id")?,
            email: row.try_get("primary_email")?,
            display_name: row.try_get("display_name")?,
        })
    }

    pub(crate) async fn load_account_identity_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<AccountIdentity> {
        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        Ok(AccountIdentity {
            id: row.try_get("id")?,
            email: row.try_get("primary_email")?,
            display_name: row.try_get("display_name")?,
        })
    }

    pub(crate) async fn load_account_identity_by_email_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        email: &str,
    ) -> Result<AccountIdentity> {
        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE tenant_id = $1 AND lower(primary_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(email)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("grantee account not found in the same tenant"))?;

        Ok(AccountIdentity {
            id: row.try_get("id")?,
            email: row.try_get("primary_email")?,
            display_name: row.try_get("display_name")?,
        })
    }

    async fn ensure_same_tenant_account_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<()> {
        self.load_account_identity_in_tx(tx, tenant_id, account_id)
            .await
            .map(|_| ())
    }

    async fn has_sender_right_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM sender_delegation_grants
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND grantee_account_id = $3
                  AND sender_right = $4
            )
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .bind(sender_right.as_str())
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into)
    }

    async fn resolve_submission_authorization_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        input: &SubmitMessageInput,
    ) -> Result<ResolvedSubmissionAuthorization> {
        let owner = self
            .load_account_identity_in_tx(tx, tenant_id, input.account_id)
            .await?;
        let submitted_by = self
            .load_account_identity_in_tx(tx, tenant_id, input.submitted_by_account_id)
            .await?;
        let requested_from = normalize_email(&input.from_address);
        let requested_sender = input
            .sender_address
            .as_deref()
            .map(normalize_email)
            .filter(|value| !value.is_empty());
        let owner_display_name = owner.display_name.clone();
        let submitted_by_display_name = submitted_by.display_name.clone();

        if requested_from.is_empty() {
            bail!("from_address is required");
        }

        if owner.id == submitted_by.id {
            if requested_from != owner.email {
                bail!("from email must match authenticated account");
            }
            if let Some(sender_address) = requested_sender {
                if sender_address != submitted_by.email {
                    bail!("sender email must match authenticated account");
                }
            }
            return Ok(ResolvedSubmissionAuthorization {
                submitted_by,
                from_address: requested_from,
                from_display: trim_optional_text(input.from_display.as_deref())
                    .or_else(|| Some(owner_display_name.clone())),
                sender_address: None,
                sender_display: None,
                authorization_kind: SenderAuthorizationKind::SelfSend,
            });
        }

        if requested_from != owner.email {
            bail!("from email must match delegated mailbox");
        }

        if let Some(sender_address) = requested_sender {
            if sender_address != submitted_by.email {
                bail!("sender email must match authenticated account");
            }
            if !self
                .has_sender_right_in_tx(
                    tx,
                    tenant_id,
                    owner.id,
                    submitted_by.id,
                    SenderDelegationRight::SendOnBehalf,
                )
                .await?
            {
                bail!("send on behalf is not granted for this mailbox");
            }
            return Ok(ResolvedSubmissionAuthorization {
                submitted_by,
                from_address: requested_from,
                from_display: trim_optional_text(input.from_display.as_deref())
                    .or_else(|| Some(owner_display_name.clone())),
                sender_address: Some(sender_address),
                sender_display: trim_optional_text(input.sender_display.as_deref())
                    .or_else(|| Some(submitted_by_display_name)),
                authorization_kind: SenderAuthorizationKind::SendOnBehalf,
            });
        }

        if !self
            .has_sender_right_in_tx(
                tx,
                tenant_id,
                owner.id,
                submitted_by.id,
                SenderDelegationRight::SendAs,
            )
            .await?
        {
            bail!("send as is not granted for this mailbox");
        }

        Ok(ResolvedSubmissionAuthorization {
            submitted_by,
            from_address: requested_from,
            from_display: trim_optional_text(input.from_display.as_deref())
                .or_else(|| Some(owner_display_name)),
            sender_address: None,
            sender_display: None,
            authorization_kind: SenderAuthorizationKind::SendAs,
        })
    }

    pub async fn find_submission_account_by_email_in_same_tenant(
        &self,
        reference_account_id: Uuid,
        email: &str,
    ) -> Result<Option<SubmissionAccountIdentity>> {
        let tenant_id = self.tenant_id_for_account_id(reference_account_id).await?;
        let normalized_email = normalize_email(email);
        if normalized_email.is_empty() {
            return Ok(None);
        }

        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE tenant_id = $1 AND lower(primary_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(&normalized_email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| SubmissionAccountIdentity {
            account_id: row.get("id"),
            email: row.get("primary_email"),
            display_name: row.get("display_name"),
        }))
    }

    async fn delete_draft_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<()> {
        self.allocate_mail_modseq_in_tx(tx, tenant_id, account_id)
            .await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM messages m
            USING mailboxes mb
            WHERE m.mailbox_id = mb.id
              AND m.tenant_id = $1
              AND m.account_id = $2
              AND m.id = $3
              AND mb.role = 'drafts'
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("draft not found");
        }

        Ok(())
    }
}

fn map_mailbox_delegation_grant(row: MailboxDelegationGrantRow) -> MailboxDelegationGrant {
    MailboxDelegationGrant {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_sender_delegation_grant(row: SenderDelegationGrantRow) -> SenderDelegationGrant {
    SenderDelegationGrant {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        sender_right: row.sender_right,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

pub(crate) fn normalize_visible_recipients(
    input: &SubmitMessageInput,
) -> Vec<(&'static str, SubmittedRecipientInput)> {
    let mut recipients = Vec::new();
    push_recipients(&mut recipients, "to", &input.to);
    push_recipients(&mut recipients, "cc", &input.cc);
    recipients
}

pub(crate) fn normalize_bcc_recipients(input: &SubmitMessageInput) -> Vec<SubmittedRecipientInput> {
    let mut recipients = Vec::new();
    push_bcc_recipients(&mut recipients, &input.bcc);
    recipients
}

pub(crate) fn push_recipients(
    output: &mut Vec<(&'static str, SubmittedRecipientInput)>,
    kind: &'static str,
    input: &[SubmittedRecipientInput],
) {
    for recipient in input {
        let address = normalize_email(&recipient.address);
        if address.is_empty() {
            continue;
        }

        output.push((
            kind,
            SubmittedRecipientInput {
                address,
                display_name: recipient
                    .display_name
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            },
        ));
    }
}

fn push_bcc_recipients(
    output: &mut Vec<SubmittedRecipientInput>,
    input: &[SubmittedRecipientInput],
) {
    for recipient in input {
        let address = normalize_email(&recipient.address);
        if address.is_empty() {
            continue;
        }

        output.push(SubmittedRecipientInput {
            address,
            display_name: recipient
                .display_name
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        });
    }
}

pub(crate) fn participants_normalized(
    from_address: &str,
    recipients: &[(&'static str, SubmittedRecipientInput)],
) -> String {
    let mut participants = Vec::with_capacity(recipients.len() + 1);
    participants.push(from_address.to_string());
    participants.extend(
        recipients
            .iter()
            .map(|(_, recipient)| recipient.address.clone()),
    );
    participants.join(" ")
}

pub(crate) fn sender_authorization_kind_from_str(value: &str) -> SenderAuthorizationKind {
    match value.trim() {
        "send-as" => SenderAuthorizationKind::SendAs,
        "send-on-behalf" => SenderAuthorizationKind::SendOnBehalf,
        _ => SenderAuthorizationKind::SelfSend,
    }
}

pub(crate) fn sender_identity_id(kind: SenderAuthorizationKind, owner_account_id: Uuid) -> String {
    format!("{}:{}", kind.as_str(), owner_account_id)
}

#[cfg(test)]
mod tests {
    use super::{canonical_submission_phases, CanonicalSubmissionPhase};

    #[test]
    fn canonical_submission_persists_sent_before_queue_handoff() {
        assert_eq!(
            canonical_submission_phases(false),
            vec![
                CanonicalSubmissionPhase::EnsureSentMailbox,
                CanonicalSubmissionPhase::PersistSentMessage,
                CanonicalSubmissionPhase::PersistOutboundQueue,
            ]
        );
    }

    #[test]
    fn draft_submission_deletes_source_only_after_queue_persistence() {
        assert_eq!(
            canonical_submission_phases(true),
            vec![
                CanonicalSubmissionPhase::EnsureSentMailbox,
                CanonicalSubmissionPhase::PersistSentMessage,
                CanonicalSubmissionPhase::PersistOutboundQueue,
                CanonicalSubmissionPhase::DeleteSourceDraft,
            ]
        );
    }
}
