use anyhow::Result;
use serde::Serialize;
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{sha256_hex, submission::AttachmentUploadInput, Storage};

#[derive(Debug, Clone, Serialize)]
pub struct ClientAttachment {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub size: String,
}

#[derive(Debug)]
struct StoredAttachmentBlob {
    id: Uuid,
    domain_id: Uuid,
}

impl Storage {
    pub(crate) async fn ingest_message_attachments_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        message_id: Uuid,
        attachments: &[AttachmentUploadInput],
    ) -> Result<Vec<Uuid>> {
        if attachments.is_empty() {
            return Ok(Vec::new());
        }

        let domain_id = self
            .load_account_domain_id_in_tx(tx, tenant_id, account_id)
            .await?;
        let mut attachment_ids = Vec::with_capacity(attachments.len());

        for (ordinal, attachment) in attachments.iter().enumerate() {
            let attachment_id = Uuid::new_v4();
            let blob = self
                .store_attachment_blob_in_tx(
                    tx,
                    tenant_id,
                    domain_id,
                    attachment.media_type.trim(),
                    attachment.file_name.trim(),
                    &attachment.blob_bytes,
                )
                .await?;

            let mime_part_id = Uuid::new_v4();
            sqlx::query(
                r#"
                INSERT INTO mime_parts (
                    id, tenant_id, message_id, domain_id, part_path, ordinal,
                    content_type, content_disposition, content_id, file_name, size_octets, blob_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                "#,
            )
            .bind(mime_part_id)
            .bind(tenant_id)
            .bind(message_id)
            .bind(blob.domain_id)
            .bind(format!("attachment.{}", ordinal + 1))
            .bind(ordinal as i32)
            .bind(attachment.media_type.trim())
            .bind(attachment_disposition(attachment.disposition.as_deref()))
            .bind(attachment.content_id.as_deref().map(str::trim))
            .bind(attachment.file_name.trim())
            .bind(attachment.blob_bytes.len() as i64)
            .bind(blob.id)
            .execute(&mut **tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO attachments (
                    id, tenant_id, account_id, message_id, domain_id, mime_part_id,
                    blob_id, file_name, disposition, content_id, ordinal, size_octets
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                "#,
            )
            .bind(attachment_id)
            .bind(tenant_id)
            .bind(account_id)
            .bind(message_id)
            .bind(blob.domain_id)
            .bind(mime_part_id)
            .bind(blob.id)
            .bind(attachment.file_name.trim())
            .bind(attachment_disposition(attachment.disposition.as_deref()))
            .bind(attachment.content_id.as_deref().map(str::trim))
            .bind(ordinal as i32)
            .bind(attachment.blob_bytes.len() as i64)
            .execute(&mut **tx)
            .await?;
            attachment_ids.push(attachment_id);
        }

        sqlx::query(
            r#"
            UPDATE messages
            SET has_attachments = TRUE
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;

        Ok(attachment_ids)
    }

    async fn store_attachment_blob_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        domain_id: Uuid,
        media_type: &str,
        file_name: &str,
        blob_bytes: &[u8],
    ) -> Result<StoredAttachmentBlob> {
        let content_sha256 = sha256_hex(blob_bytes);

        if let Some(row) = sqlx::query(
            r#"
            SELECT id
            FROM blobs
            WHERE tenant_id = $1
              AND domain_id = $2
              AND blob_kind = 'attachment'
              AND content_sha256 = $3
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(domain_id)
        .bind(&content_sha256)
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(StoredAttachmentBlob {
                id: row.try_get("id")?,
                domain_id,
            });
        }

        let blob_id = Uuid::new_v4();
        let extraction_status = if supports_attachment_text_extraction(media_type, file_name) {
            "queued"
        } else {
            "unsupported"
        };
        sqlx::query(
            r#"
            INSERT INTO blobs (
                id, tenant_id, domain_id, blob_kind, content_sha256,
                media_type, size_octets, blob_bytes, magika_status,
                extraction_status, validated_at
            )
            VALUES ($1, $2, $3, 'attachment', $4, $5, $6, $7, 'valid', $8, NOW())
            "#,
        )
        .bind(blob_id)
        .bind(tenant_id)
        .bind(domain_id)
        .bind(content_sha256)
        .bind(media_type)
        .bind(blob_bytes.len() as i64)
        .bind(blob_bytes)
        .bind(extraction_status)
        .execute(&mut **tx)
        .await?;

        if extraction_status == "queued" {
            sqlx::query(
                r#"
                INSERT INTO attachment_extraction_jobs (id, tenant_id, blob_id, status)
                VALUES ($1, $2, $3, 'queued')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(blob_id)
            .execute(&mut **tx)
            .await?;
        }

        Ok(StoredAttachmentBlob {
            id: blob_id,
            domain_id,
        })
    }
}

pub(crate) fn supports_attachment_text_extraction(media_type: &str, file_name: &str) -> bool {
    let media_type = media_type.trim().to_ascii_lowercase();
    let file_name = file_name.trim().to_ascii_lowercase();
    matches!(
        media_type.as_str(),
        "application/pdf"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.oasis.opendocument.text"
    ) || file_name.ends_with(".pdf")
        || file_name.ends_with(".docx")
        || file_name.ends_with(".odt")
}

fn attachment_disposition(value: Option<&str>) -> &'static str {
    match value.map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("inline") => "inline",
        _ => "attachment",
    }
}

pub(crate) fn attachment_kind(media_type: &str, name: &str) -> String {
    let lower_media = media_type.to_lowercase();
    let lower_name = name.to_lowercase();
    if lower_media.contains("pdf") || lower_name.ends_with(".pdf") {
        "PDF".to_string()
    } else if lower_media.contains("word")
        || lower_name.ends_with(".docx")
        || lower_name.ends_with(".doc")
    {
        "DOCX".to_string()
    } else if lower_media.contains("opendocument") || lower_name.ends_with(".odt") {
        "ODT".to_string()
    } else {
        attachment_extension_label(name)
            .or_else(|| media_type_label(&lower_media))
            .unwrap_or_else(|| "FILE".to_string())
    }
}

fn attachment_extension_label(name: &str) -> Option<String> {
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension.trim())
        .filter(|extension| !extension.is_empty())?;
    let normalized = extension
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase();
    if normalized.is_empty() || normalized.len() > 8 {
        return None;
    }
    Some(normalized)
}

fn media_type_label(media_type: &str) -> Option<String> {
    let subtype = media_type
        .split_once('/')
        .map(|(_, subtype)| subtype)
        .unwrap_or(media_type)
        .split(';')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let normalized = subtype
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase();
    if normalized.is_empty() || normalized.len() > 8 {
        return None;
    }
    Some(normalized)
}

#[cfg(test)]
mod tests {
    use super::supports_attachment_text_extraction;

    #[test]
    fn extraction_queue_scope_is_limited_to_document_text_formats() {
        assert!(supports_attachment_text_extraction(
            "application/pdf",
            "report.bin"
        ));
        assert!(supports_attachment_text_extraction(
            "application/octet-stream",
            "brief.docx"
        ));
        assert!(supports_attachment_text_extraction(
            "application/vnd.oasis.opendocument.text",
            "notes.data"
        ));

        assert!(!supports_attachment_text_extraction(
            "image/png",
            "diagram.png"
        ));
        assert!(!supports_attachment_text_extraction(
            "text/plain",
            "notes.txt"
        ));
    }
}
