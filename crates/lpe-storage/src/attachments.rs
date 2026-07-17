use std::collections::HashMap;

use anyhow::Result;
use serde::Serialize;
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    blob_store::{DurableBlobKind, PostgresBlobStore, PutBlobRequest, StoredBlobRef},
    mapi_events::MapiEventCustomPropertyValue,
    submission::AttachmentUploadInput,
    ActiveSyncAttachment, ActiveSyncAttachmentContent, AuditEntryInput, CanonicalChangeCategory,
    JmapEmail, JmapUploadBlob, Storage,
};

#[derive(Debug, Clone, Serialize)]
pub struct ClientAttachment {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub size: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CalendarEventAttachment {
    pub id: Uuid,
    pub event_id: Uuid,
    pub file_name: String,
    pub media_type: String,
    pub size_octets: u64,
    pub file_reference: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiEventAttachmentUpsert {
    pub attach_num: u32,
    pub attachment: AttachmentUploadInput,
    pub custom_property_upserts: Vec<MapiEventCustomPropertyValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MapiEventAttachmentChanges {
    pub upserts: Vec<MapiEventAttachmentUpsert>,
    pub delete_attachment_ids: Vec<Uuid>,
}

impl Storage {
    pub(crate) async fn insert_calendar_event_attachment_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        calendar_id: Uuid,
        event_id: Uuid,
        ordinal: i32,
        attachment: &AttachmentUploadInput,
    ) -> Result<CalendarEventAttachment> {
        let domain_id = self
            .load_account_domain_id_in_tx(tx, tenant_id, owner_account_id)
            .await?;
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
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(calendar_id)
        .bind(event_id)
        .bind(blob.domain_id)
        .bind(blob.id)
        .bind(attachment.file_name.trim())
        .bind(attachment.media_type.trim())
        .bind(attachment_disposition(attachment.disposition.as_deref()))
        .bind(normalize_attachment_content_id(attachment.content_id.as_deref()).as_deref())
        .bind(ordinal)
        .bind(attachment.blob_bytes.len() as i64)
        .execute(&mut **tx)
        .await?;

        Ok(CalendarEventAttachment {
            id: attachment_id,
            event_id,
            file_name: attachment.file_name.trim().to_string(),
            media_type: attachment.media_type.trim().to_string(),
            size_octets: attachment.blob_bytes.len() as u64,
            file_reference: calendar_attachment_file_reference(event_id, attachment_id),
        })
    }

    pub(crate) async fn delete_calendar_event_attachment_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        event_id: Uuid,
        attachment_id: Uuid,
    ) -> Result<()> {
        let deleted = sqlx::query_scalar::<_, Uuid>(
            r#"
            DELETE FROM calendar_event_attachments
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND event_id = $3
              AND id = $4
            RETURNING id
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(event_id)
        .bind(attachment_id)
        .fetch_optional(&mut **tx)
        .await?;
        if deleted.is_none() {
            anyhow::bail!("calendar Event attachment was not found in the parent transaction");
        }
        Ok(())
    }

    pub(crate) async fn fetch_calendar_event_attachments_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        event_id: Uuid,
    ) -> Result<Vec<CalendarEventAttachment>> {
        let rows = sqlx::query(
            r#"
            SELECT
                attachment.id,
                attachment.event_id,
                attachment.file_name,
                attachment.media_type,
                attachment.size_octets
            FROM calendar_event_attachments attachment
            JOIN calendar_events event
              ON event.tenant_id = attachment.tenant_id
             AND event.owner_account_id = attachment.owner_account_id
             AND event.id = attachment.event_id
             AND event.lifecycle_state IN ('active', 'deleted')
            WHERE attachment.tenant_id = $1
              AND attachment.owner_account_id = $2
              AND attachment.event_id = $3
            ORDER BY attachment.ordinal ASC, attachment.id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(event_id)
        .fetch_all(&mut **tx)
        .await?;
        rows.into_iter()
            .map(calendar_event_attachment_from_row)
            .collect()
    }

    pub(crate) async fn apply_mapi_event_attachment_changes_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        calendar_id: Uuid,
        event_id: Uuid,
        changes: &MapiEventAttachmentChanges,
    ) -> Result<Vec<CalendarEventAttachment>> {
        for upsert in &changes.upserts {
            let stored = self
                .insert_calendar_event_attachment_in_tx(
                    tx,
                    tenant_id,
                    owner_account_id,
                    calendar_id,
                    event_id,
                    upsert.attach_num as i32,
                    &upsert.attachment,
                )
                .await?;
            replace_attachment_custom_properties_in_tx(
                tx,
                tenant_id,
                owner_account_id,
                stored.id,
                &upsert.custom_property_upserts,
            )
            .await?;
        }
        for attachment_id in &changes.delete_attachment_ids {
            sqlx::query(
                r#"
                DELETE FROM mapi_custom_property_values
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND object_kind = 'attachment'
                  AND canonical_id = $3
                "#,
            )
            .bind(tenant_id)
            .bind(owner_account_id)
            .bind(attachment_id)
            .execute(&mut **tx)
            .await?;
            Self::delete_calendar_event_attachment_in_tx(
                tx,
                tenant_id,
                owner_account_id,
                event_id,
                *attachment_id,
            )
            .await?;
        }
        Self::fetch_calendar_event_attachments_in_tx(tx, tenant_id, owner_account_id, event_id)
            .await
    }

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
            SELECT
                attachment.id,
                attachment.event_id,
                attachment.file_name,
                attachment.media_type,
                attachment.size_octets
            FROM calendar_event_attachments attachment
            JOIN calendar_events event
              ON event.tenant_id = attachment.tenant_id
             AND event.owner_account_id = attachment.owner_account_id
             AND event.id = attachment.event_id
             AND event.lifecycle_state = 'active'
            WHERE attachment.tenant_id = $1
              AND attachment.owner_account_id = $2
              AND attachment.event_id = $3
            ORDER BY attachment.ordinal ASC, attachment.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(calendar_event_attachment_from_row)
            .collect()
    }

    pub async fn fetch_calendar_attachments_for_events(
        &self,
        account_id: Uuid,
        event_ids: &[Uuid],
    ) -> Result<Vec<(Uuid, Vec<CalendarEventAttachment>)>> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                attachment.id,
                attachment.event_id,
                attachment.file_name,
                attachment.media_type,
                attachment.size_octets
            FROM calendar_event_attachments attachment
            JOIN calendar_events event
              ON event.tenant_id = attachment.tenant_id
             AND event.owner_account_id = attachment.owner_account_id
             AND event.id = attachment.event_id
             AND event.lifecycle_state IN ('active', 'deleted')
            WHERE attachment.tenant_id = $1
              AND attachment.owner_account_id = $2
              AND attachment.event_id = ANY($3)
            ORDER BY attachment.event_id, attachment.ordinal, attachment.id
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_ids)
        .fetch_all(&self.pool)
        .await?;
        let mut attachments_by_event = HashMap::new();
        for row in rows {
            let attachment = calendar_event_attachment_from_row(row)?;
            attachments_by_event
                .entry(attachment.event_id)
                .or_insert_with(Vec::new)
                .push(attachment);
        }
        Ok(event_ids
            .iter()
            .map(|event_id| {
                (
                    *event_id,
                    attachments_by_event.remove(event_id).unwrap_or_default(),
                )
            })
            .collect())
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
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
              AND lifecycle_state = 'active'
            LIMIT 1
            FOR UPDATE
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
        self.advance_calendar_event_version_in_tx(
            &mut tx, &tenant_id, account_id, event_id, modseq,
        )
        .await?;
        let affected_principals = Self::calendar_event_affected_principals_in_tx(
            &mut tx, &tenant_id, account_id, event_id,
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
            &affected_principals,
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
            JOIN calendar_events event
              ON event.tenant_id = a.tenant_id
             AND event.owner_account_id = a.owner_account_id
             AND event.id = a.event_id
             AND event.lifecycle_state = 'active'
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
        let event_exists = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
              AND lifecycle_state = 'active'
            FOR UPDATE
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .fetch_optional(&mut *tx)
        .await?;
        if event_exists.is_none() {
            tx.commit().await?;
            return Ok(None);
        }
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
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
              AND lifecycle_state = 'active'
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
        self.advance_calendar_event_version_in_tx(
            &mut tx, &tenant_id, account_id, event_id, modseq,
        )
        .await?;
        let affected_principals = Self::calendar_event_affected_principals_in_tx(
            &mut tx, &tenant_id, account_id, event_id,
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
            &affected_principals,
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

    pub async fn add_message_attachment(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> Result<Option<(JmapEmail, ActiveSyncAttachment)>> {
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
        let exists = sqlx::query(
            r#"
            SELECT id
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND visibility = 'visible'
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(existing_membership) = exists else {
            tx.commit().await?;
            return Ok(None);
        };

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let attachment_ids = self
            .ingest_message_attachments_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                message_id,
                &[attachment],
            )
            .await?;
        let Some(attachment_id) = attachment_ids.into_iter().next() else {
            tx.commit().await?;
            return Ok(None);
        };

        self.assign_message_attachments_membership_in_tx(
            &mut tx,
            &tenant_id,
            account_id,
            message_id,
            existing_membership.try_get("id")?,
        )
        .await?;

        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "attachment",
            attachment_id,
            "created",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "attachmentId": attachment_id
            }),
        )
        .await?;

        sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET modseq = $3, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $4 AND message_id = $2
              AND visibility = 'visible'
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(modseq)
        .bind(account_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        let email = self
            .fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("message not found after attachment creation"))?;
        let attachment = self
            .fetch_activesync_message_attachments(account_id, message_id)
            .await?
            .into_iter()
            .find(|attachment| attachment.id == attachment_id)
            .ok_or_else(|| anyhow::anyhow!("attachment not found after creation"))?;

        Ok(Some((email, attachment)))
    }

    pub async fn delete_message_attachment(
        &self,
        account_id: Uuid,
        file_reference: &str,
        audit: AuditEntryInput,
    ) -> Result<Option<JmapEmail>> {
        let Some((message_id, attachment_id)) =
            crate::parse_activesync_file_reference(file_reference)
        else {
            return Ok(None);
        };
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;

        let deleted = sqlx::query(
            r#"
            DELETE FROM attachments a
            WHERE a.tenant_id = $1
              AND a.id = $2
              AND a.message_id = $3
              AND a.account_id = $4
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = a.tenant_id
                    AND mm.account_id = a.account_id
                    AND mm.message_id = a.message_id
                    AND mm.visibility = 'visible'
              )
            RETURNING a.message_id
            "#,
        )
        .bind(&tenant_id)
        .bind(attachment_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;

        if deleted.is_none() {
            tx.commit().await?;
            return Ok(None);
        }

        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "attachment",
            attachment_id,
            "destroyed",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "attachmentId": attachment_id
            }),
        )
        .await?;

        sqlx::query(
            r#"
            UPDATE messages
            SET
                has_attachments = EXISTS (
                    SELECT 1
                    FROM attachments
                    WHERE tenant_id = $1 AND message_id = $2
                )
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET modseq = $3, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $4 AND message_id = $2
              AND visibility = 'visible'
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(modseq)
        .bind(account_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        Ok(self
            .fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next())
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

fn calendar_event_attachment_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<CalendarEventAttachment> {
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
}

async fn replace_attachment_custom_properties_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    attachment_id: Uuid,
    values: &[MapiEventCustomPropertyValue],
) -> Result<()> {
    for value in values {
        sqlx::query(
            r#"
            INSERT INTO mapi_custom_property_values (
                tenant_id, account_id, object_kind, canonical_id,
                property_tag, property_type, property_value
            )
            VALUES ($1, $2, 'attachment', $3, $4, $5, $6)
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(attachment_id)
        .bind(i64::from(value.property_tag))
        .bind(i32::from(value.property_type))
        .bind(&value.property_value)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(crate) fn validate_mapi_event_attachment_changes(
    changes: &MapiEventAttachmentChanges,
) -> Result<()> {
    let mut attach_nums = std::collections::HashSet::new();
    for upsert in &changes.upserts {
        if upsert.attach_num > i32::MAX as u32 {
            anyhow::bail!("MAPI Event attachment number exceeds the canonical ordinal range");
        }
        if !attach_nums.insert(upsert.attach_num) {
            anyhow::bail!("MAPI Event attachment upserts contain a duplicate attachment number");
        }
        if upsert.attachment.file_name.trim().is_empty()
            || upsert.attachment.media_type.trim().is_empty()
        {
            anyhow::bail!("MAPI Event attachment file name and media type are required");
        }
        let mut custom_tags = std::collections::HashSet::new();
        for value in &upsert.custom_property_upserts {
            if value.property_type != (value.property_tag & 0xFFFF) as u16 {
                anyhow::bail!("MAPI attachment custom property type does not match its tag");
            }
            if !custom_tags.insert(value.property_tag) {
                anyhow::bail!("MAPI attachment custom properties contain a duplicate tag");
            }
        }
    }
    let mut deletes = std::collections::HashSet::new();
    for attachment_id in &changes.delete_attachment_ids {
        if attachment_id.is_nil() || !deletes.insert(*attachment_id) {
            anyhow::bail!("MAPI Event attachment deletes contain an invalid attachment id");
        }
    }
    Ok(())
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
