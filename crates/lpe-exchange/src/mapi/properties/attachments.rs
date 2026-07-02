use super::*;

pub(in crate::mapi) fn attachment_property_value(
    attachment: &MapiAttachment,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    match property_tag {
        PID_TAG_ATTACH_NUM => Some(MapiValue::U32(attachment.attach_num)),
        PID_TAG_DISPLAY_NAME_W | PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
            Some(MapiValue::String(attachment.file_name.clone()))
        }
        PID_TAG_ATTACH_EXTENSION_W => Some(MapiValue::String(attachment_file_extension(
            &attachment.file_name,
        ))),
        PID_TAG_ATTACH_MIME_TAG_W => Some(MapiValue::String(attachment.media_type.clone())),
        PID_TAG_ATTACH_SIZE => Some(MapiValue::U32(
            attachment.size_octets.min(u64::from(u32::MAX)) as u32,
        )),
        PID_TAG_ATTACH_METHOD => Some(MapiValue::U32(attachment_method_value(attachment))),
        PID_TAG_RENDERING_POSITION => Some(MapiValue::U32(u32::MAX)),
        PID_TAG_ATTACHMENT_FLAGS | PID_TAG_ATTACHMENT_LINK_ID => Some(MapiValue::U32(0)),
        PID_TAG_ATTACH_FLAGS => Some(MapiValue::U32(if attachment.content_id.is_some() {
            4
        } else {
            0
        })),
        PID_TAG_ATTACHMENT_HIDDEN => Some(MapiValue::Bool(attachment_is_inline(attachment))),
        PID_TAG_ATTACH_CONTENT_ID_W => Some(MapiValue::String(
            attachment.content_id.clone().unwrap_or_default(),
        )),
        PID_TAG_ATTACH_RENDERING => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => Some(MapiValue::U64(0)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            attachment.file_reference.as_bytes().to_vec(),
        )),
        _ => None,
    }
}

pub(in crate::mapi) fn attachment_is_inline(attachment: &MapiAttachment) -> bool {
    attachment
        .disposition
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("inline"))
        || attachment.content_id.is_some()
}

pub(in crate::mapi) fn attachment_is_embedded_message(attachment: &MapiAttachment) -> bool {
    attachment_metadata_is_embedded_message(&attachment.media_type, &attachment.file_name)
}

pub(in crate::mapi) fn attachment_metadata_is_embedded_message(
    media_type: &str,
    file_name: &str,
) -> bool {
    media_type
        .trim()
        .eq_ignore_ascii_case("application/vnd.ms-outlook")
        || file_name.trim().to_ascii_lowercase().ends_with(".msg")
}

pub(in crate::mapi) fn attachment_method_value(attachment: &MapiAttachment) -> u32 {
    if attachment_is_embedded_message(attachment) {
        ATTACH_EMBEDDED_MESSAGE
    } else {
        ATTACH_BY_VALUE
    }
}

pub(in crate::mapi) fn attachment_method_value_from_metadata(
    media_type: &str,
    file_name: &str,
) -> u32 {
    if attachment_metadata_is_embedded_message(media_type, file_name) {
        ATTACH_EMBEDDED_MESSAGE
    } else {
        ATTACH_BY_VALUE
    }
}

pub(in crate::mapi) fn attachment_file_extension(file_name: &str) -> String {
    let file_name = file_name.trim();
    file_name
        .rsplit_once('.')
        .filter(|(base, ext)| !base.is_empty() && !ext.is_empty())
        .map(|(_, ext)| format!(".{ext}"))
        .unwrap_or_default()
}

pub(in crate::mapi) fn pending_attachment_upload(
    attach_num: u32,
    properties: &HashMap<u32, MapiValue>,
    data: Vec<u8>,
) -> AttachmentUploadInput {
    let content_id = optional_pending_text_property(properties, &[PID_TAG_ATTACH_CONTENT_ID_W])
        .map(|value| value.trim().trim_matches(['<', '>']).to_string())
        .filter(|value| !value.is_empty());
    let hidden = properties
        .get(&PID_TAG_ATTACHMENT_HIDDEN)
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    AttachmentUploadInput {
        file_name: pending_attachment_file_name(attach_num, properties),
        media_type: pending_attachment_media_type(properties),
        disposition: Some(
            if content_id.is_some() || hidden {
                "inline"
            } else {
                "attachment"
            }
            .to_string(),
        ),
        content_id,
        blob_bytes: data,
    }
}

pub(in crate::mapi) fn pending_attachment_file_name(
    attach_num: u32,
    properties: &HashMap<u32, MapiValue>,
) -> String {
    optional_pending_text_property(
        properties,
        &[PID_TAG_ATTACH_LONG_FILENAME_W, PID_TAG_ATTACH_FILENAME_W],
    )
    .unwrap_or_else(|| format!("mapi-attachment-{attach_num}.bin"))
}

pub(in crate::mapi) fn pending_attachment_media_type(
    properties: &HashMap<u32, MapiValue>,
) -> String {
    optional_pending_text_property(properties, &[PID_TAG_ATTACH_MIME_TAG_W])
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

pub(in crate::mapi) fn mapi_expected_attachment_kind(
    media_type: &str,
    file_name: &str,
) -> ExpectedKind {
    let media_type = media_type.trim().to_ascii_lowercase();
    let file_name = file_name.trim().to_ascii_lowercase();
    if matches!(
        media_type.as_str(),
        "application/pdf"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.oasis.opendocument.text"
    ) || file_name.ends_with(".pdf")
        || file_name.ends_with(".docx")
        || file_name.ends_with(".odt")
    {
        ExpectedKind::SupportedAttachmentText
    } else {
        ExpectedKind::Any
    }
}
