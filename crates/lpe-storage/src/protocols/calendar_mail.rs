use std::collections::HashMap;

use anyhow::Result;
use sqlx::{PgPool, Postgres, Row};
use uuid::Uuid;

use crate::blob_store::{DurableBlobKind, PostgresBlobStore};
use crate::mail::{
    parse_calendar_meeting_request, parse_calendar_meeting_response_with_content_sha256,
};
use crate::{AttachmentUploadInput, CalendarMeetingRequest, CalendarMeetingResponse};

const CALENDAR_PARTS_QUERY: &str = r#"
        SELECT
            part.message_id,
            part.id AS mime_part_id,
            message.authorized_calendar_response_content_sha256,
            message.calendar_response_processed,
            part.domain_id,
            part.content_type,
            part.is_scheduling_body,
            part.blob_id,
            part.blob_kind,
            blob.content_sha256 AS blob_content_sha256,
            calendar_attachment.id AS attachment_id,
            calendar_attachment.disposition AS attachment_disposition
        FROM mime_parts part
        JOIN messages message
         ON message.tenant_id = part.tenant_id
         AND message.id = part.message_id
        JOIN blobs blob
          ON blob.tenant_id = part.tenant_id
         AND blob.domain_id = part.domain_id
         AND blob.id = part.blob_id
         AND blob.blob_kind = part.blob_kind
        LEFT JOIN attachments calendar_attachment
          ON calendar_attachment.tenant_id = part.tenant_id
         AND calendar_attachment.account_id = $2
         AND calendar_attachment.message_id = part.message_id
         AND calendar_attachment.mime_part_id = part.id
        WHERE part.tenant_id = $1
          AND part.message_id = ANY($3)
          AND lower(btrim(split_part(part.content_type, ';', 1))) = 'text/calendar'
          AND part.is_scheduling_body
        ORDER BY
            part.message_id ASC,
            CASE
                WHEN lower(COALESCE(calendar_attachment.disposition, '')) = 'inline'
                THEN 0
                ELSE 1
            END ASC,
            part.ordinal ASC
        "#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CalendarMailFingerprint {
    mime_part_id: Uuid,
    domain_id: Uuid,
    content_type: String,
    blob_id: Uuid,
    blob_kind: String,
    blob_content_sha256: String,
    attachment_id: Option<Uuid>,
    attachment_disposition: Option<String>,
    authorized_response_content_sha256: Option<String>,
    response_processed: bool,
}

pub(super) async fn fetch_calendar_mail_metadata(
    pool: &PgPool,
    tenant_id: Uuid,
    account_id: Uuid,
    message_ids: &[Uuid],
) -> Result<(
    HashMap<Uuid, CalendarMeetingRequest>,
    HashMap<Uuid, CalendarMeetingResponse>,
    HashMap<Uuid, Uuid>,
    HashMap<Uuid, CalendarMailFingerprint>,
)> {
    let calendar_parts = sqlx::query(CALENDAR_PARTS_QUERY)
        .bind(tenant_id)
        .bind(account_id)
        .bind(message_ids)
        .fetch_all(pool)
        .await?;
    let mut requests = HashMap::new();
    let mut responses = HashMap::new();
    let mut scheduling_parts = HashMap::new();
    let mut fingerprints = HashMap::new();
    for part in calendar_parts {
        let message_id: Uuid = part.try_get("message_id")?;
        let mime_part_id: Uuid = part.try_get("mime_part_id")?;
        scheduling_parts.insert(message_id, mime_part_id);
        let authorized_calendar_response_content_sha256: Option<String> =
            part.try_get("authorized_calendar_response_content_sha256")?;
        let calendar_response_processed: bool = part.try_get("calendar_response_processed")?;
        let domain_id: Uuid = part.try_get("domain_id")?;
        let content_type: String = part.try_get("content_type")?;
        let is_scheduling_body: bool = part.try_get("is_scheduling_body")?;
        let blob_id: Option<Uuid> = part.try_get("blob_id")?;
        let blob_kind: Option<String> = part.try_get("blob_kind")?;
        let blob_content_sha256: String = part.try_get("blob_content_sha256")?;
        let attachment_id: Option<Uuid> = part.try_get("attachment_id")?;
        let attachment_disposition: Option<String> = part.try_get("attachment_disposition")?;
        let (Some(blob_id), Some(blob_kind)) = (blob_id, blob_kind) else {
            continue;
        };
        fingerprints.insert(
            message_id,
            CalendarMailFingerprint {
                mime_part_id,
                domain_id,
                content_type: content_type.clone(),
                blob_id,
                blob_kind: blob_kind.clone(),
                blob_content_sha256,
                attachment_id,
                attachment_disposition: attachment_disposition.clone(),
                authorized_response_content_sha256: authorized_calendar_response_content_sha256
                    .clone(),
                response_processed: calendar_response_processed,
            },
        );
        let Some(kind) = durable_blob_kind(&blob_kind) else {
            continue;
        };
        let Some(blob) = PostgresBlobStore
            .read_durable_blob(pool, &tenant_id, domain_id, kind, blob_id)
            .await?
        else {
            continue;
        };
        let attachment = AttachmentUploadInput {
            file_name: String::new(),
            media_type: content_type,
            disposition: attachment_disposition.clone(),
            content_id: None,
            is_scheduling_body,
            blob_bytes: blob.bytes,
        };
        if !requests.contains_key(&message_id) {
            if let Some(mut request) =
                parse_calendar_meeting_request(std::slice::from_ref(&attachment))
            {
                request.transport_attachment_id = attachment_id.filter(|_| is_scheduling_body);
                requests.insert(message_id, request);
            }
        }
        if !responses.contains_key(&message_id) {
            if let Some((mut response, _)) = parse_calendar_meeting_response_with_content_sha256(
                std::slice::from_ref(&attachment),
            )
            .filter(|(_, content_sha256)| {
                response_content_is_authorized(
                    authorized_calendar_response_content_sha256.as_deref(),
                    content_sha256,
                )
            }) {
                response.transport_attachment_id = attachment_id.filter(|_| is_scheduling_body);
                response.server_processed = calendar_response_processed;
                responses.insert(message_id, response);
            }
        }
    }
    Ok((requests, responses, scheduling_parts, fingerprints))
}

pub(super) async fn fetch_calendar_mail_fingerprints_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    message_ids: &[Uuid],
) -> Result<HashMap<Uuid, CalendarMailFingerprint>> {
    let rows = sqlx::query(CALENDAR_PARTS_QUERY)
        .bind(tenant_id)
        .bind(account_id)
        .bind(message_ids)
        .fetch_all(&mut **tx)
        .await?;
    let mut fingerprints = HashMap::new();
    for row in rows {
        let message_id: Uuid = row.try_get("message_id")?;
        fingerprints.insert(
            message_id,
            CalendarMailFingerprint {
                mime_part_id: row.try_get("mime_part_id")?,
                domain_id: row.try_get("domain_id")?,
                content_type: row.try_get("content_type")?,
                blob_id: row.try_get("blob_id")?,
                blob_kind: row.try_get("blob_kind")?,
                blob_content_sha256: row.try_get("blob_content_sha256")?,
                attachment_id: row.try_get("attachment_id")?,
                attachment_disposition: row.try_get("attachment_disposition")?,
                authorized_response_content_sha256: row
                    .try_get("authorized_calendar_response_content_sha256")?,
                response_processed: row.try_get("calendar_response_processed")?,
            },
        );
    }
    Ok(fingerprints)
}

fn response_content_is_authorized(authorized: Option<&str>, content_sha256: &str) -> bool {
    authorized == Some(content_sha256)
}

fn durable_blob_kind(blob_kind: &str) -> Option<DurableBlobKind> {
    match blob_kind {
        "attachment" => Some(DurableBlobKind::Attachment),
        "mime_part" => Some(DurableBlobKind::MimePart),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{durable_blob_kind, response_content_is_authorized, CALENDAR_PARTS_QUERY};
    use crate::blob_store::DurableBlobKind;

    #[test]
    fn calendar_metadata_query_loads_durable_blob_identity_not_database_bytes() {
        for column in [
            "part.id AS mime_part_id",
            "message.authorized_calendar_response_content_sha256",
            "message.calendar_response_processed",
            "part.domain_id",
            "part.blob_id",
            "part.blob_kind",
            "blob.content_sha256 AS blob_content_sha256",
        ] {
            assert!(CALENDAR_PARTS_QUERY.contains(column));
        }
        assert!(!CALENDAR_PARTS_QUERY.contains("blob.blob_bytes"));
        assert!(CALENDAR_PARTS_QUERY.contains("AND part.is_scheduling_body"));
        assert!(CALENDAR_PARTS_QUERY
            .contains("lower(btrim(split_part(part.content_type, ';', 1))) = 'text/calendar'"));
        assert!(CALENDAR_PARTS_QUERY.contains("JOIN messages message"));
    }

    #[test]
    fn calendar_metadata_accepts_both_durable_mime_part_blob_kinds() {
        assert_eq!(
            durable_blob_kind("attachment"),
            Some(DurableBlobKind::Attachment)
        );
        assert_eq!(
            durable_blob_kind("mime_part"),
            Some(DurableBlobKind::MimePart)
        );
        assert_eq!(durable_blob_kind("raw_message"), None);
    }

    #[test]
    fn calendar_response_repair_requires_the_exact_authorized_content_hash() {
        let hash = "1".repeat(64);
        let other_hash = "2".repeat(64);
        assert!(response_content_is_authorized(Some(&hash), &hash));
        assert!(!response_content_is_authorized(None, &hash));
        assert!(!response_content_is_authorized(Some(&other_hash), &hash));
    }
}
