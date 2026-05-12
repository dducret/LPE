use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{domain_from_email, normalize_email, sha256_hex, AuditEntryInput, Storage};
pub(crate) const PLATFORM_TENANT_ID: &str = "__platform__";
pub(crate) const MAX_SIEVE_SCRIPT_BYTES: usize = 64 * 1024;
pub(crate) const MAX_SIEVE_SCRIPTS_PER_ACCOUNT: i64 = 16;
pub(crate) const DEFAULT_COLLECTION_ID: &str = "default";
pub(crate) const DEFAULT_TASK_LIST_NAME: &str = "Tasks";
pub(crate) const DEFAULT_TASK_LIST_ROLE: &str = "inbox";
pub(crate) const CANONICAL_CHANGE_CHANNEL: &str = "lpe_canonical_changes";
pub(crate) const EXPECTED_SCHEMA_VERSION: &str = "0.3.0-sql-v2";

impl Storage {
    pub(crate) async fn allocate_mail_modseq_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<i64> {
        self.allocate_account_modseq_in_tx(tx, tenant_id, account_id, "mail")
            .await
    }

    pub(crate) async fn allocate_account_modseq_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        category: &str,
    ) -> Result<i64> {
        let modseq = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO account_sync_state (tenant_id, account_id, category, current_modseq)
            VALUES ($1, $2, $3, 2)
            ON CONFLICT (tenant_id, account_id, category)
            DO UPDATE SET
                current_modseq = account_sync_state.current_modseq + 1,
                updated_at = NOW()
            RETURNING current_modseq
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(category)
        .fetch_one(&mut **tx)
        .await?;

        Ok(modseq)
    }

    pub(crate) async fn ensure_account_exists(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<()> {
        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await?;

        if account_exists.is_none() {
            bail!("account not found");
        }

        Ok(())
    }

    pub(crate) async fn ensure_mailbox(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        role: &str,
        display_name: &str,
        sort_order: i32,
        _retention_days: i32,
    ) -> Result<Uuid> {
        let lookup_role = if role.trim().is_empty() {
            "custom"
        } else {
            role.trim()
        };
        if let Some(row) = sqlx::query(
            r#"
            SELECT id
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND role = $3
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(lookup_role)
        .fetch_optional(&mut **tx)
        .await?
        {
            return row.try_get("id").map_err(Into::into);
        }

        let mailbox_id = Uuid::new_v4();
        let uid_validity = allocate_uid_validity();
        sqlx::query(
            r#"
            INSERT INTO mailboxes (
                id, tenant_id, account_id, role, display_name, sort_order, uid_validity
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(mailbox_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(lookup_role)
        .bind(display_name)
        .bind(sort_order)
        .bind(uid_validity)
        .execute(&mut **tx)
        .await?;

        Ok(mailbox_id)
    }

    pub(crate) async fn insert_mail_change_log_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Option<Uuid>,
        mailbox_id: Option<Uuid>,
        object_kind: &str,
        object_id: Uuid,
        change_kind: &str,
        modseq: i64,
        affected_principal_ids: &[Uuid],
        summary_json: Value,
    ) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO mail_change_log (
                tenant_id, account_id, mailbox_id, object_kind, object_id, change_kind,
                modseq, affected_principal_ids, summary_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING cursor
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(object_kind)
        .bind(object_id)
        .bind(change_kind)
        .bind(modseq)
        .bind(dedup_sorted_uuids(affected_principal_ids))
        .bind(summary_json)
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn affected_mail_principals_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<Vec<Uuid>> {
        let mut principals = vec![account_id];
        let delegated = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT grantee_account_id
            FROM mailbox_delegation_grants
            WHERE tenant_id = $1 AND owner_account_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_all(&mut **tx)
        .await?;
        principals.extend(delegated);
        principals.sort();
        principals.dedup();
        Ok(principals)
    }

    pub(crate) async fn allocate_mailbox_membership_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_id: Uuid,
        thread_id: Uuid,
        received_at_sql: &str,
        is_seen: bool,
        is_flagged: bool,
        is_draft: bool,
        change_kind: &str,
    ) -> Result<Uuid> {
        let modseq = self
            .allocate_mail_modseq_in_tx(tx, tenant_id, account_id)
            .await?;
        let row = sqlx::query(
            r#"
            UPDATE mailboxes
            SET uid_next = uid_next + 1,
                modseq = GREATEST(modseq + 1, $4),
                total_messages = total_messages + 1,
                unread_messages = unread_messages + CASE WHEN $5 THEN 0 ELSE 1 END,
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            RETURNING uid_next - 1 AS imap_uid
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(modseq)
        .bind(is_seen)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;
        let imap_uid: i64 = row.try_get("imap_uid")?;
        let membership_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO mailbox_messages (
                id, tenant_id, account_id, mailbox_id, message_id, thread_id,
                imap_uid, modseq, is_seen, is_flagged, is_draft, received_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, COALESCE($12::timestamptz, NOW())
            )
            "#,
        )
        .bind(membership_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(message_id)
        .bind(thread_id)
        .bind(imap_uid)
        .bind(modseq)
        .bind(is_seen)
        .bind(is_flagged)
        .bind(is_draft)
        .bind(if received_at_sql.trim().is_empty() {
            None::<&str>
        } else {
            Some(received_at_sql)
        })
        .execute(&mut **tx)
        .await?;

        let principals = Self::affected_mail_principals_in_tx(tx, tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            tx,
            tenant_id,
            Some(account_id),
            Some(mailbox_id),
            "mailbox_message",
            membership_id,
            change_kind,
            modseq,
            &principals,
            json!({
                "messageId": message_id,
                "mailboxId": mailbox_id,
                "threadId": thread_id,
                "imapUid": imap_uid
            }),
        )
        .await?;
        Ok(membership_id)
    }

    pub(crate) async fn load_account_domain_id_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<Uuid> {
        sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT primary_domain_id
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("account not found"))
    }

    pub(crate) async fn store_message_blob_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        domain_id: Uuid,
        blob_kind: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<Uuid> {
        let content_sha256 = sha256_hex(bytes);
        if let Some(blob_id) = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM blobs
            WHERE tenant_id = $1
              AND domain_id = $2
              AND blob_kind = $3
              AND content_sha256 = $4
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(domain_id)
        .bind(blob_kind)
        .bind(&content_sha256)
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(blob_id);
        }

        let blob_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO blobs (
                id, tenant_id, domain_id, blob_kind, content_sha256,
                media_type, size_octets, blob_bytes
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(blob_id)
        .bind(tenant_id)
        .bind(domain_id)
        .bind(blob_kind)
        .bind(content_sha256)
        .bind(media_type)
        .bind(bytes.len() as i64)
        .bind(bytes)
        .execute(&mut **tx)
        .await?;

        Ok(blob_id)
    }

    pub(crate) async fn upsert_message_body_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        domain_id: Uuid,
        message_id: Uuid,
        body_text: &str,
        body_html_sanitized: Option<&str>,
    ) -> Result<()> {
        sqlx::query("DELETE FROM message_bodies WHERE tenant_id = $1 AND message_id = $2")
            .bind(tenant_id)
            .bind(message_id)
            .execute(&mut **tx)
            .await?;
        sqlx::query("DELETE FROM mime_parts WHERE tenant_id = $1 AND message_id = $2")
            .bind(tenant_id)
            .bind(message_id)
            .execute(&mut **tx)
            .await?;

        let text_part_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO mime_parts (
                id, tenant_id, message_id, domain_id, part_path, ordinal,
                content_type, size_octets
            )
            VALUES ($1, $2, $3, $4, '1', 0, 'text/plain; charset=utf-8', $5)
            "#,
        )
        .bind(text_part_id)
        .bind(tenant_id)
        .bind(message_id)
        .bind(domain_id)
        .bind(body_text.len() as i64)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                id, tenant_id, message_id, mime_part_id, body_kind,
                body_text, sanitized_html, content_hash, search_vector
            )
            VALUES ($1, $2, $3, $4, 'text', $5, NULL, $6, to_tsvector('simple', $5))
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(message_id)
        .bind(text_part_id)
        .bind(body_text)
        .bind(sha256_hex(body_text.as_bytes()))
        .execute(&mut **tx)
        .await?;

        if let Some(html) = body_html_sanitized.filter(|value| !value.trim().is_empty()) {
            let html_part_id = Uuid::new_v4();
            sqlx::query(
                r#"
                INSERT INTO mime_parts (
                    id, tenant_id, message_id, domain_id, part_path, ordinal,
                    content_type, size_octets
                )
                VALUES ($1, $2, $3, $4, '2', 1, 'text/html; charset=utf-8', $5)
                "#,
            )
            .bind(html_part_id)
            .bind(tenant_id)
            .bind(message_id)
            .bind(domain_id)
            .bind(html.len() as i64)
            .execute(&mut **tx)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO message_bodies (
                    id, tenant_id, message_id, mime_part_id, body_kind,
                    body_text, sanitized_html, content_hash, search_vector
                )
                VALUES ($1, $2, $3, $4, 'html', $5, $5, $6, to_tsvector('simple', $5))
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(message_id)
            .bind(html_part_id)
            .bind(html)
            .bind(sha256_hex(html.as_bytes()))
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn replace_message_headers_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        message_id: Uuid,
        raw_message: &[u8],
    ) -> Result<usize> {
        sqlx::query("DELETE FROM message_headers WHERE tenant_id = $1 AND message_id = $2")
            .bind(tenant_id)
            .bind(message_id)
            .execute(&mut **tx)
            .await?;

        let headers = crate::mail::parse_header_records(raw_message);
        for (ordinal, header) in headers.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_headers (
                    id, tenant_id, message_id, header_name, header_value, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(message_id)
            .bind(header.name.trim())
            .bind(header.value.trim())
            .bind(ordinal as i32)
            .execute(&mut **tx)
            .await?;
        }

        Ok(headers.len())
    }

    pub(crate) async fn assign_message_attachments_membership_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        message_id: Uuid,
        mailbox_message_id: Uuid,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE attachments
            SET mailbox_message_id = $4
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND mailbox_message_id IS NULL
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(mailbox_message_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(crate) async fn upsert_mail_search_document_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        mailbox_message_id: Uuid,
        message_id: Uuid,
        subject_text: &str,
        participants_visible: &str,
        body_text: &str,
        attachment_text: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO mail_search_documents (
                tenant_id, account_id, mailbox_message_id, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                to_tsvector('simple', concat_ws(' ', $5, $6, $7, $8))
            )
            ON CONFLICT (tenant_id, account_id, mailbox_message_id)
            DO UPDATE SET
                subject_text = EXCLUDED.subject_text,
                participants_visible = EXCLUDED.participants_visible,
                body_text = EXCLUDED.body_text,
                attachment_text = EXCLUDED.attachment_text,
                search_vector = EXCLUDED.search_vector,
                updated_at = NOW()
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_message_id)
        .bind(message_id)
        .bind(subject_text)
        .bind(participants_visible)
        .bind(body_text)
        .bind(attachment_text)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(crate) async fn insert_audit(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        audit: AuditEntryInput,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO audit_events (id, tenant_id, actor, action, subject)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(audit.actor)
        .bind(audit.action)
        .bind(audit.subject)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub(crate) async fn tenant_id_for_domain_name(&self, domain_name: &str) -> Result<String> {
        let domain_name = domain_name.trim().to_lowercase();
        if domain_name.is_empty() {
            bail!("domain name is required");
        }

        let existing = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM domains
            WHERE lower(name) = lower($1)
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&domain_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(existing.unwrap_or(domain_name))
    }

    pub(crate) async fn tenant_id_for_domain_id(&self, domain_id: Uuid) -> Result<String> {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM domains
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(domain_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("domain not found"))
    }

    pub(crate) async fn tenant_id_for_account_id(&self, account_id: Uuid) -> Result<String> {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM accounts
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))
    }

    pub(crate) async fn tenant_id_for_account_email(&self, email: &str) -> Result<String> {
        let email = normalize_email(email);
        if email.is_empty() {
            bail!("account email is required");
        }

        if let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM accounts
            WHERE lower(primary_email) = lower($1)
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(tenant_id);
        }

        let domain = domain_from_email(&email)?;
        self.tenant_id_for_domain_name(&domain).await
    }

    pub(crate) async fn tenant_id_for_admin_email(&self, email: &str) -> Result<String> {
        let email = normalize_email(email);
        if email.is_empty() {
            bail!("admin email is required");
        }

        if let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM server_administrators
            WHERE lower(email) = lower($1)
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(tenant_id);
        }

        if let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM admin_credentials
            WHERE lower(email) = lower($1)
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(tenant_id);
        }

        Ok(PLATFORM_TENANT_ID.to_string())
    }
}

pub(crate) fn allocate_uid_validity() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().max(1) as i64)
        .unwrap_or(1)
}

fn dedup_sorted_uuids(values: &[Uuid]) -> Vec<Uuid> {
    let mut values = values.to_vec();
    values.sort();
    values.dedup();
    values
}

#[cfg(test)]
mod tests {
    use crate::attachments::attachment_kind;
    use crate::pst::validate_pst_import_path;
    use crate::submission::{
        normalize_bcc_recipients, normalize_visible_recipients, participants_normalized,
    };
    use crate::{
        default_permissions_for_role, domain_from_email, normalize_admin_permissions,
        normalize_admin_session_auth_method, normalize_task_status, SubmitMessageInput,
        SubmittedRecipientInput,
    };
    use lpe_magika::{
        write_validation_record, ExpectedKind, IngressContext, PolicyDecision, ValidationOutcome,
        ValidationRequest,
    };
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };
    use uuid::Uuid;

    fn submit_input() -> SubmitMessageInput {
        SubmitMessageInput {
            draft_message_id: None,
            account_id: Uuid::nil(),
            submitted_by_account_id: Uuid::nil(),
            source: "test".to_string(),
            from_display: None,
            from_address: "sender@example.test".to_string(),
            sender_display: None,
            sender_address: None,
            to: vec![SubmittedRecipientInput {
                address: "to@example.test".to_string(),
                display_name: None,
            }],
            cc: vec![SubmittedRecipientInput {
                address: "cc@example.test".to_string(),
                display_name: Some("  CC Person  ".to_string()),
            }],
            bcc: vec![SubmittedRecipientInput {
                address: "bcc@example.test".to_string(),
                display_name: Some("  Hidden Person  ".to_string()),
            }],
            subject: "subject".to_string(),
            body_text: "body".to_string(),
            body_html_sanitized: None,
            internet_message_id: None,
            mime_blob_ref: None,
            size_octets: 0,
            unread: None,
            flagged: None,
            attachments: Vec::new(),
        }
    }

    #[test]
    fn visible_recipients_exclude_bcc() {
        let recipients = normalize_visible_recipients(&submit_input());

        assert_eq!(recipients.len(), 2);
        assert_eq!(recipients[0].0, "to");
        assert_eq!(recipients[0].1.address, "to@example.test");
        assert_eq!(recipients[1].0, "cc");
        assert_eq!(recipients[1].1.address, "cc@example.test");
        assert_eq!(recipients[1].1.display_name.as_deref(), Some("CC Person"));
    }

    #[test]
    fn bcc_recipients_are_kept_separately() {
        let recipients = normalize_bcc_recipients(&submit_input());

        assert_eq!(recipients.len(), 1);
        assert_eq!(recipients[0].address, "bcc@example.test");
        assert_eq!(recipients[0].display_name.as_deref(), Some("Hidden Person"));
    }

    #[test]
    fn participants_normalized_ignores_bcc_addresses() {
        let visible = normalize_visible_recipients(&submit_input());
        let participants = participants_normalized("sender@example.test", &visible);

        assert!(participants.contains("sender@example.test"));
        assert!(participants.contains("to@example.test"));
        assert!(participants.contains("cc@example.test"));
        assert!(!participants.contains("bcc@example.test"));
    }

    #[test]
    fn participants_normalized_remains_visible_only_even_with_bcc_display_name() {
        let input = submit_input();
        let visible = normalize_visible_recipients(&input);
        let participants = participants_normalized("sender@example.test", &visible);

        assert!(!participants.contains("Hidden Person"));
        assert!(!participants.contains("bcc@example.test"));
    }

    #[test]
    fn participants_normalized_allows_null_reverse_path() {
        let visible = normalize_visible_recipients(&submit_input());
        let participants = participants_normalized("", &visible);

        assert_eq!(participants, "to@example.test cc@example.test");
    }

    #[test]
    fn pst_processing_requires_prior_validation_record() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("lpe-pst-validation-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let pst_path = dir.join("mailbox.pst");
        fs::write(&pst_path, b"LPE-PST-V1\n").unwrap();

        assert!(validate_pst_import_path(&pst_path).is_err());

        let outcome = ValidationOutcome {
            detected_label: "pst".to_string(),
            detected_mime: "application/vnd.ms-outlook".to_string(),
            description: "pst".to_string(),
            group: "archive".to_string(),
            extensions: vec!["pst".to_string()],
            score: Some(0.99),
            declared_mime: Some("application/vnd.ms-outlook".to_string()),
            filename: Some("mailbox.pst".to_string()),
            mismatch: false,
            policy_decision: PolicyDecision::Accept,
            reason: "file validated".to_string(),
        };
        write_validation_record(
            &pst_path,
            &ValidationRequest {
                ingress_context: IngressContext::PstUpload,
                declared_mime: Some("application/vnd.ms-outlook".to_string()),
                filename: Some("mailbox.pst".to_string()),
                expected_kind: ExpectedKind::Pst,
            },
            &outcome,
            fs::metadata(&pst_path).unwrap().len(),
        )
        .unwrap();

        std::env::set_var("LPE_MAGIKA_BIN", "missing-magika-binary-for-test");
        let result = validate_pst_import_path(&pst_path);
        std::env::remove_var("LPE_MAGIKA_BIN");
        assert!(result.is_err());
    }

    #[test]
    fn domain_dedup_scope_comes_from_account_email_domain() {
        assert_eq!(
            domain_from_email("Alice@Example.Test").unwrap(),
            "example.test"
        );
    }

    #[test]
    fn task_status_defaults_to_needs_action() {
        assert_eq!(normalize_task_status("").unwrap(), "needs-action");
    }

    #[test]
    fn task_status_accepts_vtodo_aligned_values() {
        assert_eq!(
            normalize_task_status("needs-action").unwrap(),
            "needs-action"
        );
        assert_eq!(normalize_task_status("in-progress").unwrap(), "in-progress");
        assert_eq!(normalize_task_status("completed").unwrap(), "completed");
        assert_eq!(normalize_task_status("cancelled").unwrap(), "cancelled");
    }

    #[test]
    fn task_status_rejects_unknown_values() {
        assert!(normalize_task_status("done").is_err());
    }

    #[test]
    fn attachment_kind_falls_back_to_real_extension_label() {
        assert_eq!(
            attachment_kind("application/octet-stream", "archive.zip"),
            "ZIP"
        );
        assert_eq!(attachment_kind("application/octet-stream", "blob"), "FILE");
    }

    #[test]
    fn built_in_role_permissions_include_dashboard() {
        let permissions = default_permissions_for_role("tenant-admin");

        assert!(permissions
            .iter()
            .any(|permission| permission == "dashboard"));
        assert!(permissions.iter().any(|permission| permission == "domains"));
        assert!(!permissions
            .iter()
            .any(|permission| permission == "antispam"));
        assert!(!permissions.iter().any(|permission| permission == "*"));

        let transport_permissions = default_permissions_for_role("transport-operator");
        assert!(!transport_permissions
            .iter()
            .any(|permission| permission == "antispam"));
    }

    #[test]
    fn explicit_permissions_are_normalized_and_deduplicated() {
        let permissions = normalize_admin_permissions(
            "custom",
            "mail, dashboard, mail",
            &[
                " dashboard ".to_string(),
                "audit".to_string(),
                String::new(),
                "mail".to_string(),
            ],
        );

        assert_eq!(permissions, vec!["audit", "dashboard", "mail"]);
    }

    #[test]
    fn admin_session_auth_method_collapses_totp_to_password_family() {
        assert_eq!(normalize_admin_session_auth_method("password"), "password");
        assert_eq!(
            normalize_admin_session_auth_method("password+totp"),
            "password"
        );
        assert_eq!(normalize_admin_session_auth_method("oidc"), "oidc");
    }
}
