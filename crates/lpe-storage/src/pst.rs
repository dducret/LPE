use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_magika::{
    read_validation_record, ExpectedKind, IngressContext, PolicyDecision, ValidationRequest,
    Validator,
};
use serde::Serialize;
use sqlx::{FromRow, Postgres, Row};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use uuid::Uuid;

use crate::{
    blob_store::{DurableBlobKind, PostgresBlobStore},
    preview_text, AttachmentUploadInput, Storage,
};

#[derive(Debug, Clone, Serialize)]
pub struct PstTransferJobRecord {
    pub id: Uuid,
    pub direction: String,
    pub server_path: String,
    pub status: String,
    pub requested_by: String,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub processed_messages: u32,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewPstTransferJob {
    pub mailbox_id: Uuid,
    pub direction: String,
    pub server_path: String,
    pub requested_by: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PstJobExecutionSummary {
    pub processed_jobs: u32,
    pub completed_jobs: u32,
    pub failed_jobs: u32,
}

#[derive(Debug, FromRow)]
pub(crate) struct PstTransferJobRow {
    pub(crate) id: Uuid,
    pub(crate) mailbox_id: Uuid,
    pub(crate) direction: String,
    pub(crate) server_path: String,
    pub(crate) status: String,
    pub(crate) requested_by: String,
    pub(crate) created_at: String,
    pub(crate) completed_at: Option<String>,
    pub(crate) processed_messages: i32,
    pub(crate) error_message: Option<String>,
}

#[derive(Debug, FromRow)]
pub(crate) struct PendingPstJobRow {
    pub(crate) id: Uuid,
    pub(crate) tenant_id: String,
    pub(crate) mailbox_id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) direction: String,
    pub(crate) server_path: String,
    pub(crate) requested_by: String,
}

#[derive(Debug)]
pub(crate) struct PstImportedMessage {
    pub(crate) internet_message_id: String,
    pub(crate) from_address: String,
    pub(crate) subject: String,
    pub(crate) body_text: String,
    pub(crate) attachments: Vec<AttachmentUploadInput>,
}

impl Storage {
    pub async fn process_pending_pst_jobs(&self) -> Result<PstJobExecutionSummary> {
        let jobs = sqlx::query_as::<_, PendingPstJobRow>(
            r#"
            SELECT
                j.id,
                j.tenant_id,
                j.mailbox_id,
                mb.account_id,
                j.direction,
                j.server_path,
                j.requested_by
            FROM mailbox_pst_jobs j
            JOIN mailboxes mb ON mb.id = j.mailbox_id
            WHERE j.status IN ('requested', 'failed')
            ORDER BY j.created_at ASC
            LIMIT 10
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut summary = PstJobExecutionSummary {
            processed_jobs: 0,
            completed_jobs: 0,
            failed_jobs: 0,
        };

        for job in jobs {
            summary.processed_jobs += 1;
            if let Err(error) = self.mark_pst_job_running(&job.tenant_id, job.id).await {
                summary.failed_jobs += 1;
                let _ = self
                    .mark_pst_job_failed(
                        &job.tenant_id,
                        job.id,
                        &format!("cannot start job: {error}"),
                    )
                    .await;
                continue;
            }

            let result = if job.direction == "export" {
                self.export_mailbox_to_pst(&job).await
            } else {
                self.import_mailbox_from_pst(&job).await
            };

            match result {
                Ok(processed_messages) => {
                    self.mark_pst_job_completed(&job.tenant_id, job.id, processed_messages)
                        .await?;
                    summary.completed_jobs += 1;
                }
                Err(error) => {
                    self.mark_pst_job_failed(&job.tenant_id, job.id, &error.to_string())
                        .await?;
                    summary.failed_jobs += 1;
                }
            }
        }

        Ok(summary)
    }

    async fn mark_pst_job_running(&self, tenant_id: &str, job_id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mailbox_pst_jobs
            SET status = 'running', error_message = NULL
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn mark_pst_job_completed(
        &self,
        tenant_id: &str,
        job_id: Uuid,
        processed_messages: u32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mailbox_pst_jobs
            SET status = 'completed',
                processed_messages = $3,
                error_message = NULL,
                completed_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(job_id)
        .bind(processed_messages as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn mark_pst_job_failed(
        &self,
        tenant_id: &str,
        job_id: Uuid,
        error_message: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mailbox_pst_jobs
            SET status = 'failed',
                error_message = $3,
                completed_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(job_id)
        .bind(error_message.chars().take(1000).collect::<String>())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn export_mailbox_to_pst(&self, job: &PendingPstJobRow) -> Result<u32> {
        ensure_parent_directory(&job.server_path)?;
        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                m.internet_message_id,
                COALESCE(fr.address, '') AS from_address,
                m.normalized_subject AS subject_normalized,
                COALESCE(tb.body_text, '') AS body_text
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
             AND mm.visibility <> 'expunged'
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id
             AND fr.message_id = m.id
             AND fr.role = 'from'
            LEFT JOIN LATERAL (
                SELECT body_text
                FROM message_bodies
                WHERE tenant_id = m.tenant_id
                  AND message_id = m.id
                  AND body_kind = 'text'
                ORDER BY id ASC
                LIMIT 1
            ) tb ON TRUE
            WHERE m.tenant_id = $1 AND mm.mailbox_id = $2
            ORDER BY mm.received_at ASC, m.received_at ASC
            "#,
        )
        .bind(&job.tenant_id)
        .bind(job.mailbox_id)
        .fetch_all(&self.pool)
        .await?;

        let mut file = File::create(&job.server_path)?;
        writeln!(file, "LPE-PST-V1")?;
        writeln!(file, "mailbox_id={}", job.mailbox_id)?;
        writeln!(file, "requested_by={}", job.requested_by)?;

        for row in &rows {
            let internet_message_id = row
                .try_get::<Option<String>, _>("internet_message_id")?
                .unwrap_or_default();
            let from_address: String = row.try_get("from_address")?;
            let subject: String = row.try_get("subject_normalized")?;
            let body_text: String = row.try_get("body_text")?;
            writeln!(
                file,
                "MESSAGE\t{}\t{}\t{}\t{}",
                encode_pst_field(&internet_message_id),
                encode_pst_field(&from_address),
                encode_pst_field(&subject),
                encode_pst_field(&body_text)
            )?;

            let attachments = sqlx::query(
                r#"
                SELECT
                    a.file_name,
                    a.domain_id,
                    a.blob_id
                FROM attachments a
                WHERE a.tenant_id = $1 AND a.message_id = $2
                ORDER BY a.file_name ASC
                "#,
            )
            .bind(&job.tenant_id)
            .bind(row.try_get::<Uuid, _>("id")?)
            .fetch_all(&self.pool)
            .await?;

            for attachment in attachments {
                let file_name: String = attachment.try_get("file_name")?;
                let Some(blob) = PostgresBlobStore
                    .read_durable_blob(
                        &self.pool,
                        &job.tenant_id,
                        attachment.try_get("domain_id")?,
                        DurableBlobKind::Attachment,
                        attachment.try_get("blob_id")?,
                    )
                    .await?
                else {
                    continue;
                };
                writeln!(
                    file,
                    "ATTACHMENT\t{}\t{}\t{}",
                    encode_pst_field(&file_name),
                    encode_pst_field(&blob.media_type),
                    BASE64.encode(blob.bytes)
                )?;
            }
        }

        Ok(rows.len() as u32)
    }

    async fn import_mailbox_from_pst(&self, job: &PendingPstJobRow) -> Result<u32> {
        validate_pst_import_path(Path::new(&job.server_path))?;

        let file = File::open(&job.server_path)?;
        let mut reader = BufReader::new(file);
        let mut header = String::new();
        reader.read_line(&mut header)?;
        if header.trim() != "LPE-PST-V1" {
            bail!("unsupported PST file for this bootstrap engine");
        }

        let mut processed_messages = 0;
        let mut pending_message: Option<PstImportedMessage> = None;
        let mut tx = self.pool.begin().await?;
        for line in reader.lines() {
            let line = line?;
            if line.starts_with("ATTACHMENT\t") {
                let parts = line.split('\t').collect::<Vec<_>>();
                if parts.len() != 4 {
                    continue;
                }
                if let Some(message) = pending_message.as_mut() {
                    message.attachments.push(AttachmentUploadInput {
                        file_name: decode_pst_field(parts[1]),
                        media_type: decode_pst_field(parts[2]),
                        disposition: Some("attachment".to_string()),
                        content_id: None,
                        blob_bytes: BASE64
                            .decode(parts[3])
                            .context("decode PST attachment payload")?,
                    });
                }
                continue;
            }

            if !line.starts_with("MESSAGE\t") {
                continue;
            }

            if let Some(message) = pending_message.take() {
                self.persist_pst_imported_message_in_tx(&mut tx, job, message)
                    .await?;
                processed_messages += 1;
            }

            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 5 {
                continue;
            }
            pending_message = Some(PstImportedMessage {
                internet_message_id: decode_pst_field(parts[1]),
                from_address: decode_pst_field(parts[2]),
                subject: decode_pst_field(parts[3]),
                body_text: decode_pst_field(parts[4]),
                attachments: Vec::new(),
            });
        }

        if let Some(message) = pending_message.take() {
            self.persist_pst_imported_message_in_tx(&mut tx, job, message)
                .await?;
            processed_messages += 1;
        }

        tx.commit().await?;
        Ok(processed_messages)
    }

    async fn persist_pst_imported_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        job: &PendingPstJobRow,
        message: PstImportedMessage,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(job.account_id).await?;
        let message_id = Uuid::new_v4();
        let preview_text = preview_text(&message.body_text);
        let modseq = self
            .allocate_mail_modseq_in_tx(tx, &tenant_id, job.account_id)
            .await?;
        let size_octets = message.body_text.len().saturating_add(
            message
                .attachments
                .iter()
                .map(|attachment| attachment.blob_bytes.len())
                .sum::<usize>(),
        ) as i64;

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
                $1, $2, $3, $4, $5, NULLIF($6, ''),
                $7, NOW(), NULL, NULL, $8, NULL,
                NULL, 'self', $3, $9, $10, TRUE, FALSE, FALSE, $11, $12,
                'pst-import', 'stored'
            )
            "#,
        )
        .bind(message_id)
        .bind(&tenant_id)
        .bind(job.account_id)
        .bind(job.mailbox_id)
        .bind(Uuid::new_v4())
        .bind(message.internet_message_id)
        .bind(modseq)
        .bind(message.from_address)
        .bind(message.subject.clone())
        .bind(preview_text)
        .bind(size_octets.max(0))
        .bind(format!("pst-import:{message_id}"))
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, NULL, '', NULL, $3, to_tsvector('simple', $4))
            "#,
        )
        .bind(message_id)
        .bind(message.body_text.clone())
        .bind(format!("pst-import:{message_id}"))
        .bind(format!("{} {}", message.subject, message.body_text))
        .execute(&mut **tx)
        .await?;

        self.ingest_message_attachments_in_tx(
            tx,
            &self.tenant_id_for_account_id(job.account_id).await?,
            job.account_id,
            message_id,
            &message.attachments,
        )
        .await?;

        Ok(())
    }
}

fn ensure_parent_directory(path: &str) -> Result<()> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

pub(crate) fn validate_pst_import_path(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)?;
    let record = read_validation_record(path).with_context(|| {
        format!(
            "missing or unreadable PST validation record for {}",
            path.display()
        )
    })?;
    if record.ingress_context != IngressContext::PstUpload {
        bail!("PST validation record has an unexpected ingress context");
    }
    if record.expected_kind != ExpectedKind::Pst {
        bail!("PST validation record does not describe a PST upload");
    }
    if record.policy_decision != PolicyDecision::Accept {
        bail!(
            "PST validation record is not accepted: {}",
            record.outcome.reason
        );
    }
    if record.file_size != metadata.len() {
        bail!("PST validation record does not match the current file size");
    }

    let outcome = Validator::from_env().validate_path(
        ValidationRequest {
            ingress_context: IngressContext::PstProcessing,
            declared_mime: record.outcome.declared_mime.clone(),
            filename: path
                .file_name()
                .and_then(|value| value.to_str())
                .map(ToString::to_string),
            expected_kind: ExpectedKind::Pst,
        },
        path,
    )?;
    if outcome.policy_decision != PolicyDecision::Accept {
        bail!(
            "PST processing blocked by Magika validation: {}",
            outcome.reason
        );
    }

    Ok(())
}

fn encode_pst_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

fn decode_pst_field(value: &str) -> String {
    let mut output = String::new();
    let mut chars = value.chars();
    while let Some(char) = chars.next() {
        if char != '\\' {
            output.push(char);
            continue;
        }

        match chars.next() {
            Some('t') => output.push('\t'),
            Some('r') => output.push('\r'),
            Some('n') => output.push('\n'),
            Some('\\') => output.push('\\'),
            Some(other) => {
                output.push('\\');
                output.push(other);
            }
            None => output.push('\\'),
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sha256_hex;
    use sqlx::postgres::PgPoolOptions;

    const SCHEMA: &str = include_str!("../sql/schema.sql");

    async fn test_storage() -> Option<Storage> {
        let database_url = match std::env::var("LPE_STORAGE_TEST_DATABASE_URL") {
            Ok(value) => value,
            Err(_) => return None,
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect to LPE_STORAGE_TEST_DATABASE_URL");
        sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .execute(&pool)
            .await
            .expect("reset test database schema");
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .expect("apply schema.sql to test database");
        Some(Storage::new(pool))
    }

    async fn insert_account_mailbox(
        storage: &Storage,
        tenant_id: Uuid,
        domain_id: Uuid,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) {
        sqlx::query(
            r#"
            INSERT INTO tenants (id, slug, display_name)
            VALUES ($1, 'pst-test', 'PST Test')
            "#,
        )
        .bind(tenant_id)
        .execute(storage.pool())
        .await
        .expect("insert tenant");
        sqlx::query(
            r#"
            INSERT INTO domains (id, tenant_id, name)
            VALUES ($1, $2, 'example.test')
            "#,
        )
        .bind(domain_id)
        .bind(tenant_id)
        .execute(storage.pool())
        .await
        .expect("insert domain");
        sqlx::query(
            r#"
            INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
            VALUES ($1, $2, $3, 'user@example.test', 'User')
            "#,
        )
        .bind(account_id)
        .bind(tenant_id)
        .bind(domain_id)
        .execute(storage.pool())
        .await
        .expect("insert account");
        sqlx::query(
            r#"
            INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, uid_validity)
            VALUES ($1, $2, $3, 'inbox', 'Inbox', 1)
            "#,
        )
        .bind(mailbox_id)
        .bind(tenant_id)
        .bind(account_id)
        .execute(storage.pool())
        .await
        .expect("insert mailbox");
    }

    async fn insert_message_with_attachment(
        storage: &Storage,
        tenant_id: Uuid,
        domain_id: Uuid,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> (Uuid, Uuid) {
        let tenant = tenant_id.to_string();
        let message_id = Uuid::new_v4();
        let mailbox_message_id = Uuid::new_v4();
        let raw_message =
            b"From: sender@example.test\r\nSubject: Export Subject\r\n\r\nExport body".to_vec();
        let mut tx = storage.pool().begin().await.expect("begin export tx");
        let raw_blob_id = storage
            .store_message_blob_in_tx(
                &mut tx,
                &tenant,
                domain_id,
                "raw_message",
                "message/rfc822",
                &raw_message,
            )
            .await
            .expect("store raw message blob");
        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, domain_id, blob_id, internet_message_id,
                message_hash, normalized_subject, received_at, size_octets
            )
            VALUES ($1, $2, $3, $4, '<export@example.test>', $5, 'Export Subject', NOW(), $6)
            "#,
        )
        .bind(message_id)
        .bind(tenant_id)
        .bind(domain_id)
        .bind(raw_blob_id)
        .bind(sha256_hex(&raw_message))
        .bind(raw_message.len() as i64)
        .execute(&mut *tx)
        .await
        .expect("insert export message");
        sqlx::query(
            r#"
            INSERT INTO message_recipients (
                id, tenant_id, message_id, role, address, display_name, ordinal
            )
            VALUES ($1, $2, $3, 'from', 'sender@example.test', 'Sender', 0)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await
        .expect("insert sender");
        storage
            .upsert_message_body_in_tx(&mut tx, &tenant, domain_id, message_id, "Export body", None)
            .await
            .expect("insert message body");
        let attachment_ids = storage
            .ingest_message_attachments_in_tx(
                &mut tx,
                &tenant,
                account_id,
                message_id,
                &[AttachmentUploadInput {
                    file_name: "export.txt".to_string(),
                    media_type: "text/plain".to_string(),
                    disposition: Some("attachment".to_string()),
                    content_id: None,
                    blob_bytes: b"attachment body".to_vec(),
                }],
            )
            .await
            .expect("ingest export attachment");
        sqlx::query(
            r#"
            INSERT INTO mailbox_messages (
                id, tenant_id, account_id, mailbox_id, message_id, imap_uid, received_at
            )
            VALUES ($1, $2, $3, $4, $5, 1, NOW())
            "#,
        )
        .bind(mailbox_message_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await
        .expect("insert mailbox message");
        storage
            .assign_message_attachments_membership_in_tx(
                &mut tx,
                &tenant,
                account_id,
                message_id,
                mailbox_message_id,
            )
            .await
            .expect("assign attachment membership");
        tx.commit().await.expect("commit export message");

        let blob_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT blob_id
            FROM attachments
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(attachment_ids[0])
        .fetch_one(storage.pool())
        .await
        .expect("load export attachment blob");
        (message_id, blob_id)
    }

    async fn insert_secondary_storage_pool(storage: &Storage) -> Uuid {
        let pool_id = Uuid::from_u128(2);
        sqlx::query(
            r#"
            INSERT INTO storage_pools (id, name, pool_kind)
            VALUES ($1, 'postgres-secondary', 'postgres')
            "#,
        )
        .bind(pool_id)
        .execute(storage.pool())
        .await
        .expect("insert secondary storage pool");
        pool_id
    }

    async fn migrate_attachment_and_cleanup_source(
        storage: &Storage,
        tenant_id: Uuid,
        domain_id: Uuid,
        blob_id: Uuid,
    ) {
        let target_pool_id = insert_secondary_storage_pool(storage).await;
        PostgresBlobStore
            .create_blob_migration_job(
                storage.pool(),
                &tenant_id.to_string(),
                domain_id,
                "attachment",
                blob_id,
                target_pool_id,
            )
            .await
            .expect("create export attachment migration job");
        let verified = PostgresBlobStore
            .copy_and_verify_one_blob_migration_job(storage.pool())
            .await
            .expect("copy and verify export attachment migration")
            .expect("verified export attachment migration");
        PostgresBlobStore
            .switch_verified_blob_migration_job(storage.pool(), verified.id)
            .await
            .expect("switch export attachment migration");
        sqlx::query(
            r#"
            UPDATE blob_placements
            SET rollback_until = NOW() - INTERVAL '1 minute'
            WHERE tenant_id = $1
              AND id = $2
              AND placement_status = 'retiring'
            "#,
        )
        .bind(tenant_id)
        .bind(verified.source_placement_id)
        .execute(storage.pool())
        .await
        .expect("expire export attachment rollback window");
        let cleanup = PostgresBlobStore
            .cleanup_one_old_placement(storage.pool(), verified.source_placement_id)
            .await
            .expect("cleanup export attachment old placement");
        assert!(cleanup.cleaned);
        assert_eq!(cleanup.status, "deleted");
    }

    #[tokio::test]
    async fn pst_export_reconstructs_attachment_after_old_placement_cleanup() {
        let Some(storage) = test_storage().await else {
            eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
            return;
        };
        let tenant_id = Uuid::new_v4();
        let domain_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        let mailbox_id = Uuid::new_v4();
        insert_account_mailbox(&storage, tenant_id, domain_id, account_id, mailbox_id).await;
        let (_message_id, attachment_blob_id) =
            insert_message_with_attachment(&storage, tenant_id, domain_id, account_id, mailbox_id)
                .await;
        migrate_attachment_and_cleanup_source(&storage, tenant_id, domain_id, attachment_blob_id)
            .await;

        let job_id = Uuid::new_v4();
        let output_path = std::env::temp_dir().join(format!("lpe-pst-export-{job_id}.pst"));
        let job = PendingPstJobRow {
            id: job_id,
            tenant_id: tenant_id.to_string(),
            mailbox_id,
            account_id,
            direction: "export".to_string(),
            server_path: output_path.to_string_lossy().to_string(),
            requested_by: "test".to_string(),
        };

        let processed = storage
            .export_mailbox_to_pst(&job)
            .await
            .expect("export mailbox after old placement cleanup");
        assert_eq!(processed, 1);
        let exported = std::fs::read_to_string(&output_path).expect("read exported PST fixture");
        let _ = std::fs::remove_file(&output_path);
        assert!(exported.contains("LPE-PST-V1"));
        assert!(exported.contains(
            "MESSAGE\t<export@example.test>\tsender@example.test\tExport Subject\tExport body"
        ));
        assert!(exported.contains(&format!(
            "ATTACHMENT\texport.txt\ttext/plain\t{}",
            BASE64.encode(b"attachment body")
        )));
    }
}
