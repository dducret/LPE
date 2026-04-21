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

use crate::{preview_text, AttachmentUploadInput, Storage};

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
                m.from_address,
                m.subject_normalized,
                COALESCE(mb.body_text, '') AS body_text
            FROM messages m
            LEFT JOIN message_bodies mb ON mb.message_id = m.id
            WHERE m.tenant_id = $1 AND m.mailbox_id = $2
            ORDER BY m.received_at ASC
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
                    a.media_type,
                    b.blob_bytes
                FROM attachments a
                JOIN attachment_blobs b ON b.id = a.attachment_blob_id
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
                let media_type: String = attachment.try_get("media_type")?;
                let blob_bytes: Vec<u8> = attachment.try_get("blob_bytes")?;
                writeln!(
                    file,
                    "ATTACHMENT\t{}\t{}\t{}",
                    encode_pst_field(&file_name),
                    encode_pst_field(&media_type),
                    BASE64.encode(blob_bytes)
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
