use anyhow::Result;
use serde::Serialize;
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    blob_store::{DurableBlobKind, PostgresBlobStore, PutBlobRequest, StoredBlobRef},
    submission::AttachmentUploadInput,
    ActiveSyncAttachmentContent, AuditEntryInput, CanonicalChangeCategory, JmapUploadBlob, Storage,
};

#[derive(Debug, Clone, Serialize)]
pub struct ClientAttachment {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub size: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CalendarEventAttachment {
    pub id: Uuid,
    pub event_id: Uuid,
    pub file_name: String,
    pub media_type: String,
    pub size_octets: u64,
    pub file_reference: String,
}

impl Storage {
    pub(crate) async fn ingest_message_attachments_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
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
            let content_id = normalize_attachment_content_id(attachment.content_id.as_deref());
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
                    content_type, content_disposition, content_id, file_name,
                    size_octets, blob_id, blob_kind
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'attachment')
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
            .bind(content_id.as_deref())
            .bind(attachment.file_name.trim())
            .bind(attachment.blob_bytes.len() as i64)
            .bind(blob.id)
            .execute(&mut **tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO attachments (
                    id, tenant_id, account_id, message_id, domain_id, mime_part_id,
                    blob_id, blob_kind, file_name, disposition, content_id, ordinal, size_octets
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'attachment', $8, $9, $10, $11, $12)
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
            .bind(content_id.as_deref())
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

    pub async fn fetch_calendar_event_attachments(
        &self,
        account_id: Uuid,
        event_id: Uuid,
    ) -> Result<Vec<CalendarEventAttachment>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT id, event_id, file_name, media_type, size_octets
            FROM calendar_event_attachments
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND event_id = $3
            ORDER BY ordinal ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let id: Uuid = row.try_get("id")?;
                let event_id: Uuid = row.try_get("event_id")?;
                Ok(CalendarEventAttachment {
                    id,
                    event_id,
                    file_name: row.try_get("file_name")?,
                    media_type: row.try_get("media_type")?,
                    size_octets: row.try_get::<i64, _>("size_octets")?.max(0) as u64,
                    file_reference: calendar_attachment_file_reference(event_id, id),
                })
            })
            .collect()
    }

    pub async fn fetch_calendar_attachments_for_events(
        &self,
        account_id: Uuid,
        event_ids: &[Uuid],
    ) -> Result<Vec<(Uuid, Vec<CalendarEventAttachment>)>> {
        let mut result = Vec::with_capacity(event_ids.len());
        for event_id in event_ids {
            result.push((
                *event_id,
                self.fetch_calendar_event_attachments(account_id, *event_id)
                    .await?,
            ));
        }
        Ok(result)
    }

    pub async fn add_calendar_event_attachment(
        &self,
        account_id: Uuid,
        event_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> Result<Option<CalendarEventAttachment>> {
        let file_name = attachment.file_name.trim();
        if file_name.is_empty() {
            anyhow::bail!("attachment file name is required");
        }
        let media_type = attachment.media_type.trim();
        if media_type.is_empty() {
            anyhow::bail!("attachment media type is required");
        }

        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let event = sqlx::query(
            r#"
            SELECT calendar_id
            FROM calendar_events
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(event) = event else {
            tx.commit().await?;
            return Ok(None);
        };
        let calendar_id: Uuid = event.try_get("calendar_id")?;
        let domain_id = self
            .load_account_domain_id_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let blob = self
            .store_attachment_blob_in_tx(
                &mut tx,
                &tenant_id,
                domain_id,
                media_type,
                file_name,
                &attachment.blob_bytes,
            )
            .await?;
        let ordinal = sqlx::query_scalar::<_, Option<i32>>(
            r#"
            SELECT COALESCE(MAX(ordinal) + 1, 0)
            FROM calendar_event_attachments
            WHERE tenant_id = $1 AND owner_account_id = $2 AND event_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .fetch_one(&mut *tx)
        .await?
        .unwrap_or(0);
        let attachment_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO calendar_event_attachments (
                id, tenant_id, owner_account_id, calendar_id, event_id, domain_id,
                blob_id, blob_kind, file_name, media_type, disposition, content_id,
                ordinal, size_octets
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'attachment', $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(attachment_id)
        .bind(&tenant_id)
        .bind(account_id)
        .bind(calendar_id)
        .bind(event_id)
        .bind(blob.domain_id)
        .bind(blob.id)
        .bind(file_name)
        .bind(media_type)
        .bind(attachment_disposition(attachment.disposition.as_deref()))
        .bind(normalize_attachment_content_id(attachment.content_id.as_deref()).as_deref())
        .bind(ordinal)
        .bind(attachment.blob_bytes.len() as i64)
        .execute(&mut *tx)
        .await?;

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "calendar_event",
            event_id,
            "updated",
            modseq,
            &[account_id],
            serde_json::json!({
                "objectUid": event_id,
                "collectionId": calendar_id,
                "attachmentChanged": true,
                "attachmentId": attachment_id
            }),
        )
        .await?;
        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            account_id,
        )
        .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        Ok(Some(CalendarEventAttachment {
            id: attachment_id,
            event_id,
            file_name: file_name.to_string(),
            media_type: media_type.to_string(),
            size_octets: attachment.blob_bytes.len() as u64,
            file_reference: calendar_attachment_file_reference(event_id, attachment_id),
        }))
    }

    pub async fn fetch_calendar_attachment_blob(
        &self,
        account_id: Uuid,
        file_reference: &str,
    ) -> Result<Option<JmapUploadBlob>> {
        let Some((event_id, attachment_id)) =
            parse_calendar_attachment_file_reference(file_reference)
        else {
            return Ok(None);
        };
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT a.id, a.file_name, a.media_type, a.domain_id, a.blob_id
            FROM calendar_event_attachments a
            WHERE a.tenant_id = $1
              AND a.owner_account_id = $2
              AND a.event_id = $3
              AND a.id = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .bind(attachment_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let Some(blob) = PostgresBlobStore
            .read_durable_blob(
                &self.pool,
                &tenant_id,
                row.try_get("domain_id")?,
                DurableBlobKind::Attachment,
                row.try_get("blob_id")?,
            )
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(JmapUploadBlob {
            id: row.try_get("id")?,
            account_id,
            media_type: blob.media_type,
            octet_size: blob.bytes.len() as u64,
            blob_bytes: blob.bytes,
        }))
    }

    pub async fn fetch_calendar_attachment_content(
        &self,
        account_id: Uuid,
        file_reference: &str,
    ) -> Result<Option<ActiveSyncAttachmentContent>> {
        let Some(blob) = self
            .fetch_calendar_attachment_blob(account_id, file_reference)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(ActiveSyncAttachmentContent {
            file_reference: file_reference.trim().to_string(),
            file_name: "calendar-attachment".to_string(),
            media_type: blob.media_type,
            blob_bytes: blob.blob_bytes,
        }))
    }

    pub async fn delete_calendar_event_attachment(
        &self,
        account_id: Uuid,
        file_reference: &str,
        audit: AuditEntryInput,
    ) -> Result<Option<Uuid>> {
        let Some((event_id, attachment_id)) =
            parse_calendar_attachment_file_reference(file_reference)
        else {
            return Ok(None);
        };
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM calendar_event_attachments
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND event_id = $3
              AND id = $4
            RETURNING event_id
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .bind(attachment_id)
        .fetch_optional(&mut *tx)
        .await?;

        if deleted.is_none() {
            tx.commit().await?;
            return Ok(None);
        }

        sqlx::query(
            r#"
            UPDATE calendar_events
            SET updated_at = NOW()
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .execute(&mut *tx)
        .await?;

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "calendar_event",
            event_id,
            "updated",
            modseq,
            &[account_id],
            serde_json::json!({
                "objectUid": event_id,
                "attachmentChanged": true,
                "attachmentId": attachment_id,
            }),
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(Some(event_id))
    }

    async fn store_attachment_blob_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        domain_id: Uuid,
        media_type: &str,
        file_name: &str,
        blob_bytes: &[u8],
    ) -> Result<StoredBlobRef> {
        let extraction_status = if supports_attachment_text_extraction(media_type, file_name) {
            "queued"
        } else {
            "unsupported"
        };
        let blob = PostgresBlobStore
            .put_durable_blob_in_tx(
                tx,
                PutBlobRequest {
                    tenant_id,
                    domain_id,
                    kind: DurableBlobKind::Attachment,
                    media_type,
                    bytes: blob_bytes,
                    magika_status: "valid",
                    extraction_status,
                    validated: true,
                },
            )
            .await?;

        if blob.created && extraction_status == "queued" {
            sqlx::query(
                r#"
                INSERT INTO attachment_extraction_jobs (id, tenant_id, blob_id, blob_kind, status)
                VALUES ($1, $2, $3, 'attachment', 'queued')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(blob.id)
            .execute(&mut **tx)
            .await?;
        }

        Ok(blob)
    }
}

pub fn calendar_attachment_file_reference(event_id: Uuid, attachment_id: Uuid) -> String {
    format!("calendar-attachment:{event_id}:{attachment_id}")
}

pub fn parse_calendar_attachment_file_reference(value: &str) -> Option<(Uuid, Uuid)> {
    let mut parts = value.trim().split(':');
    if parts.next()? != "calendar-attachment" {
        return None;
    }
    let event_id = Uuid::parse_str(parts.next()?).ok()?;
    let attachment_id = Uuid::parse_str(parts.next()?).ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((event_id, attachment_id))
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

fn normalize_attachment_content_id(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .map(|value| value.trim_matches(['<', '>']).trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
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
    use super::{normalize_attachment_content_id, supports_attachment_text_extraction};

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

    #[test]
    fn attachment_content_id_is_normalized_for_lookup() {
        assert_eq!(
            normalize_attachment_content_id(Some(" <logo@example.test> ")).as_deref(),
            Some("logo@example.test")
        );
        assert_eq!(normalize_attachment_content_id(Some(" <> ")), None);
        assert_eq!(normalize_attachment_content_id(None), None);
    }
}
