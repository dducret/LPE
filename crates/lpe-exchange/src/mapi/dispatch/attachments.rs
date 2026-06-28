use super::*;

pub(super) async fn mapi_submit_attachments_from_email<S>(
    store: &S,
    account_id: Uuid,
    email: &JmapEmail,
) -> Result<Vec<AttachmentUploadInput>>
where
    S: ExchangeStore,
{
    if !email.has_attachments {
        return Ok(Vec::new());
    }

    let attachments = store
        .fetch_message_attachments(account_id, email.id)
        .await?;
    let mut uploads = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        let Some(content) = store
            .fetch_attachment_content(account_id, &attachment.file_reference)
            .await?
        else {
            return Err(anyhow::anyhow!(
                "missing attachment content for {}",
                attachment.file_reference
            ));
        };
        uploads.push(AttachmentUploadInput {
            file_name: content.file_name,
            media_type: content.media_type,
            disposition: attachment.disposition,
            content_id: attachment.content_id,
            blob_bytes: content.blob_bytes,
        });
    }
    Ok(uploads)
}

pub(super) async fn sync_attachment_facts_for_with_embedded_content<S: ExchangeStore>(
    store: &S,
    account_id: Uuid,
    folder_id: u64,
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<mapi_mailstore::MessageAttachmentSyncFacts> {
    let mut facts = sync_attachment_facts_for(folder_id, emails, snapshot);
    for message_facts in &mut facts {
        for attachment in &mut message_facts.attachments {
            if !mapi_mailstore::attachment_sync_fact_is_embedded_message(attachment) {
                continue;
            }
            if let Ok(Some(content)) = store
                .fetch_attachment_content(account_id, &attachment.file_reference)
                .await
            {
                attachment.embedded_message_blob = Some(content.blob_bytes);
            }
        }
    }
    facts
}

pub(super) fn transient_embedded_message_id(
    folder_id: u64,
    message_id: u64,
    attach_num: u32,
) -> u64 {
    let folder_counter =
        crate::mapi::identity::global_counter_from_store_id(folder_id).unwrap_or(1);
    let message_counter =
        crate::mapi::identity::global_counter_from_store_id(message_id).unwrap_or(1);
    crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER
            .saturating_add(folder_counter)
            .saturating_add(message_counter)
            .saturating_add(u64::from(attach_num))
            .saturating_add(1),
    )
}

pub(super) fn embedded_message_open_subject(properties: &HashMap<u32, MapiValue>) -> String {
    optional_pending_text_property(
        properties,
        &[PID_TAG_NORMALIZED_SUBJECT_W, PID_TAG_SUBJECT_W],
    )
    .unwrap_or_default()
}

pub(super) async fn open_embedded_message_source<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    snapshot: &MapiMailStoreSnapshot,
    handle: u32,
    open_mode: u8,
) -> Option<(u64, u64, u32, HashMap<u32, MapiValue>)> {
    match session.handles.get(&handle)?.clone() {
        MapiObject::PendingAttachment {
            folder_id,
            message_id,
            attach_num,
            properties,
            ..
        } => {
            let attach_method = properties
                .get(&PID_TAG_ATTACH_METHOD)
                .and_then(MapiValue::as_i64)
                .unwrap_or(i64::from(ATTACH_EMBEDDED_MESSAGE));
            if attach_method != i64::from(ATTACH_EMBEDDED_MESSAGE) {
                return None;
            }
            Some((
                folder_id,
                message_id,
                attach_num,
                default_embedded_message_properties(),
            ))
        }
        MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        } => {
            if open_mode != 0 {
                return None;
            }
            let attachment = snapshot.attachment_for_message(folder_id, message_id, attach_num)?;
            if !attachment_is_embedded_message(&attachment) {
                return None;
            }
            let properties =
                embedded_message_properties_from_attachment(store, principal, &attachment).await;
            Some((folder_id, message_id, attach_num, properties))
        }
        MapiObject::SavedAttachment {
            folder_id,
            message_id,
            attach_num,
            file_reference,
            file_name,
            media_type,
            ..
        } => {
            if open_mode != 0 || !attachment_metadata_is_embedded_message(&media_type, &file_name) {
                return None;
            }
            let properties = embedded_message_properties_from_attachment_metadata(
                store,
                principal,
                &file_reference,
                &file_name,
            )
            .await;
            Some((folder_id, message_id, attach_num, properties))
        }
        _ => None,
    }
}

async fn embedded_message_properties_from_attachment<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    attachment: &crate::mapi_store::MapiAttachment,
) -> HashMap<u32, MapiValue> {
    embedded_message_properties_from_attachment_metadata(
        store,
        principal,
        &attachment.file_reference,
        &attachment.file_name,
    )
    .await
}

async fn embedded_message_properties_from_attachment_metadata<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    file_reference: &str,
    file_name: &str,
) -> HashMap<u32, MapiValue> {
    let content = store
        .fetch_attachment_content(principal.account_id, file_reference)
        .await
        .ok()
        .flatten()
        .map(|content| content.blob_bytes)
        .unwrap_or_default();
    embedded_message_properties_from_blob(file_name, &content)
}

fn default_embedded_message_properties() -> HashMap<u32, MapiValue> {
    HashMap::from([(
        PID_TAG_MESSAGE_CLASS_W,
        MapiValue::String("IPM.Note".to_string()),
    )])
}

fn embedded_message_properties_from_blob(file_name: &str, blob: &[u8]) -> HashMap<u32, MapiValue> {
    let mut properties = default_embedded_message_properties();
    let text = String::from_utf8_lossy(blob);
    if let Some(subject) = text
        .split_once("Subject:")
        .and_then(|(_, rest)| rest.split_once("\r\n").map(|(value, _)| value))
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        properties.insert(PID_TAG_SUBJECT_W, MapiValue::String(subject.to_string()));
    } else if let Some(subject) = file_name
        .trim()
        .strip_suffix(".msg")
        .filter(|value| !value.is_empty())
    {
        properties.insert(PID_TAG_SUBJECT_W, MapiValue::String(subject.to_string()));
    }
    if let Some(body_text) = text
        .split_once("Body-Length:")
        .and_then(|(_, rest)| rest.split_once("\r\n").map(|(_, body)| body))
        .map(|body| {
            body.split_once("\r\nHtml-Length:")
                .map(|(value, _)| value)
                .unwrap_or(body)
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        properties.insert(PID_TAG_BODY_W, MapiValue::String(body_text.to_string()));
    }
    properties
}

pub(super) fn pending_embedded_message_attachment_upload(
    attach_num: u32,
    attachment_properties: &HashMap<u32, MapiValue>,
    embedded_properties: &HashMap<u32, MapiValue>,
) -> AttachmentUploadInput {
    let subject = optional_pending_text_property(
        embedded_properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    )
    .unwrap_or_else(|| "Embedded message".to_string());
    let body =
        optional_pending_text_property(embedded_properties, &[PID_TAG_BODY_W]).unwrap_or_default();
    let body_html = optional_pending_text_property(embedded_properties, &[PID_TAG_BODY_HTML_W])
        .unwrap_or_default();
    let file_name = optional_pending_text_property(
        attachment_properties,
        &[PID_TAG_ATTACH_LONG_FILENAME_W, PID_TAG_ATTACH_FILENAME_W],
    )
    .unwrap_or_else(|| format!("{subject}.msg"));
    let mut payload = Vec::new();
    payload.extend_from_slice(b"LPE-MAPI-EMBEDDED-MESSAGE\0");
    payload.extend_from_slice(format!("Subject:{subject}\r\n").as_bytes());
    payload.extend_from_slice(format!("Body-Length:{}\r\n", body.len()).as_bytes());
    payload.extend_from_slice(body.as_bytes());
    payload.extend_from_slice(b"\r\nHtml-Length:");
    payload.extend_from_slice(body_html.len().to_string().as_bytes());
    payload.extend_from_slice(b"\r\n");
    payload.extend_from_slice(body_html.as_bytes());

    AttachmentUploadInput {
        file_name,
        media_type: "application/vnd.ms-outlook".to_string(),
        disposition: Some("attachment".to_string()),
        content_id: None,
        blob_bytes: if payload.is_empty() {
            format!("Embedded message {attach_num}").into_bytes()
        } else {
            payload
        },
    }
}
