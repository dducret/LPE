use anyhow::{anyhow, Result};
use lpe_attachments::extract_text_from_bytes;
use serde::Serialize;
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{domain_from_email, sha256_hex, submission::AttachmentUploadInput, Storage};

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
}

impl Storage {
    pub(crate) async fn ingest_message_attachments_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        message_id: Uuid,
        attachments: &[AttachmentUploadInput],
    ) -> Result<()> {
        if attachments.is_empty() {
            return Ok(());
        }

        let domain_name = self.load_account_domain_in_tx(tx, account_id).await?;
        let mut search_fragments = Vec::new();

        for attachment in attachments {
            let blob = self
                .store_attachment_blob_in_tx(
                    tx,
                    &domain_name,
                    attachment.media_type.trim(),
                    &attachment.blob_bytes,
                )
                .await?;
            let extracted_text = extract_supported_attachment_text(
                attachment.media_type.trim(),
                attachment.file_name.as_str(),
                &attachment.blob_bytes,
            )?;
            if let Some(text) = extracted_text
                .as_ref()
                .filter(|text| !text.trim().is_empty())
            {
                search_fragments.push(text.clone());
            }

            sqlx::query(
                r#"
                INSERT INTO attachments (
                    id, tenant_id, message_id, file_name, media_type, size_octets,
                    blob_ref, extracted_text, extracted_text_tsv, attachment_blob_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, to_tsvector('simple', COALESCE($8, '')), $9)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(message_id)
            .bind(attachment.file_name.trim())
            .bind(attachment.media_type.trim())
            .bind(attachment.blob_bytes.len() as i64)
            .bind(format!("attachment-blob:{}", blob.id))
            .bind(extracted_text)
            .bind(blob.id)
            .execute(&mut **tx)
            .await?;
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

        if !search_fragments.is_empty() {
            let attachment_text = search_fragments.join("\n");
            sqlx::query(
                r#"
                UPDATE message_bodies
                SET search_vector = to_tsvector(
                    'simple',
                    concat_ws(' ', body_text, participants_normalized, $2)
                )
                WHERE message_id = $1
                "#,
            )
            .bind(message_id)
            .bind(attachment_text)
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }

    async fn store_attachment_blob_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        domain_name: &str,
        media_type: &str,
        blob_bytes: &[u8],
    ) -> Result<StoredAttachmentBlob> {
        let content_sha256 = sha256_hex(blob_bytes);

        if let Some(row) = sqlx::query(
            r#"
            SELECT id
            FROM attachment_blobs
            WHERE tenant_id = $1 AND domain_name = $2 AND content_sha256 = $3
            LIMIT 1
            "#,
        )
        .bind(self.tenant_id_for_domain_name(domain_name).await?)
        .bind(domain_name)
        .bind(&content_sha256)
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(StoredAttachmentBlob {
                id: row.try_get("id")?,
            });
        }

        let blob_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO attachment_blobs (
                id, tenant_id, domain_name, content_sha256, media_type, size_octets, blob_bytes
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(blob_id)
        .bind(self.tenant_id_for_domain_name(domain_name).await?)
        .bind(domain_name)
        .bind(content_sha256)
        .bind(media_type)
        .bind(blob_bytes.len() as i64)
        .bind(blob_bytes)
        .execute(&mut **tx)
        .await?;

        Ok(StoredAttachmentBlob { id: blob_id })
    }

    async fn load_account_domain_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        account_id: Uuid,
    ) -> Result<String> {
        let row = sqlx::query(
            r#"
            SELECT primary_email
            FROM accounts
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;
        let primary_email: String = row.try_get("primary_email")?;
        domain_from_email(&primary_email)
    }
}

pub(crate) fn extract_supported_attachment_text(
    media_type: &str,
    file_name: &str,
    blob_bytes: &[u8],
) -> Result<Option<String>> {
    match extract_text_from_bytes(blob_bytes, Some(media_type), Some(file_name)) {
        Ok(text) => Ok(Some(text)),
        Err(error) => {
            let message = error.to_string();
            if message.contains("unsupported validated attachment format")
                || message.contains("blocked extraction")
            {
                Ok(None)
            } else {
                Err(error)
            }
        }
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
