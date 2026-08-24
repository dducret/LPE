use std::collections::HashMap;

use anyhow::Result;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::mail::{parse_calendar_meeting_request, parse_calendar_meeting_response};
use crate::{AttachmentUploadInput, CalendarMeetingRequest, CalendarMeetingResponse};

pub(super) async fn fetch_calendar_mail_metadata(
    pool: &PgPool,
    tenant_id: Uuid,
    account_id: Uuid,
    message_ids: &[Uuid],
) -> Result<(
    HashMap<Uuid, CalendarMeetingRequest>,
    HashMap<Uuid, CalendarMeetingResponse>,
)> {
    let calendar_parts = sqlx::query(
        r#"
        SELECT
            part.message_id,
            part.content_type,
            blob.blob_bytes,
            calendar_attachment.id AS attachment_id,
            calendar_attachment.disposition AS attachment_disposition
        FROM mime_parts part
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
          AND lower(part.content_type) LIKE 'text/calendar%'
        ORDER BY
            part.message_id ASC,
            CASE
                WHEN lower(COALESCE(calendar_attachment.disposition, '')) = 'inline'
                THEN 0
                ELSE 1
            END ASC,
            part.ordinal ASC
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_ids)
    .fetch_all(pool)
    .await?;
    let mut requests = HashMap::new();
    let mut responses = HashMap::new();
    for part in calendar_parts {
        let message_id: Uuid = part.try_get("message_id")?;
        let content_type: String = part.try_get("content_type")?;
        let blob_bytes: Option<Vec<u8>> = part.try_get("blob_bytes")?;
        let attachment_id: Option<Uuid> = part.try_get("attachment_id")?;
        let attachment_disposition: Option<String> = part.try_get("attachment_disposition")?;
        let Some(blob_bytes) = blob_bytes else {
            continue;
        };
        let attachment = AttachmentUploadInput {
            file_name: String::new(),
            media_type: content_type,
            disposition: attachment_disposition.clone(),
            content_id: None,
            blob_bytes,
        };
        if !requests.contains_key(&message_id) {
            if let Some(mut request) =
                parse_calendar_meeting_request(std::slice::from_ref(&attachment))
            {
                request.transport_attachment_id = attachment_id.filter(|_| {
                    attachment_disposition
                        .as_deref()
                        .is_some_and(|disposition| disposition.eq_ignore_ascii_case("inline"))
                });
                requests.insert(message_id, request);
            }
        }
        if !responses.contains_key(&message_id) {
            if let Some(response) =
                parse_calendar_meeting_response(std::slice::from_ref(&attachment))
            {
                responses.insert(message_id, response);
            }
        }
    }
    Ok((requests, responses))
}
