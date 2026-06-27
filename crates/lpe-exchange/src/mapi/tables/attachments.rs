use super::*;

pub(in crate::mapi) fn serialize_attachment_row(
    attachment: &MapiAttachment,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attachment.attach_num),
            PID_TAG_DISPLAY_NAME_W | PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &attachment.file_name)
            }
            PID_TAG_ATTACH_EXTENSION_W => {
                write_utf16z(&mut row, &attachment_file_extension(&attachment.file_name))
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &attachment.media_type),
            PID_TAG_ATTACH_SIZE => {
                write_u32(&mut row, attachment.size_octets.min(u32::MAX as u64) as u32)
            }
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, attachment_method_value(attachment)),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ATTACHMENT_FLAGS | PID_TAG_ATTACHMENT_LINK_ID => write_u32(&mut row, 0),
            PID_TAG_ATTACH_FLAGS => write_u32(
                &mut row,
                if attachment.content_id.is_some() {
                    4
                } else {
                    0
                },
            ),
            PID_TAG_ATTACHMENT_HIDDEN => row.push(if attachment_is_inline(attachment) {
                1
            } else {
                0
            }),
            PID_TAG_ATTACH_CONTENT_ID_W => {
                write_utf16z(&mut row, attachment.content_id.as_deref().unwrap_or(""))
            }
            PID_TAG_ATTACH_RENDERING => write_u16_prefixed_bytes(&mut row, &[]),
            PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => write_u64(&mut row, 0),
            PID_TAG_ENTRY_ID => {
                write_u16_prefixed_bytes(&mut row, attachment.canonical_id.as_bytes())
            }
            PID_TAG_INSTANCE_KEY => {
                write_u16_prefixed_bytes(&mut row, attachment.file_reference.as_bytes())
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_pending_attachment_row(
    attach_num: u32,
    properties: &HashMap<u32, MapiValue>,
    data: &[u8],
    columns: &[u32],
) -> Vec<u8> {
    let file_name = pending_attachment_file_name(attach_num, properties);
    let media_type = pending_attachment_media_type(properties);
    let content_id = pending_attachment_content_id(properties);
    let hidden = pending_attachment_hidden(properties);
    let size = data.len().min(u32::MAX as usize) as u32;
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) = properties.get(column) {
            write_mapi_value(&mut row, *column, value);
            continue;
        }
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_DISPLAY_NAME_W | PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &file_name)
            }
            PID_TAG_ATTACH_EXTENSION_W => {
                write_utf16z(&mut row, &attachment_file_extension(&file_name))
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size),
            PID_TAG_ATTACH_METHOD => write_u32(
                &mut row,
                attachment_method_value_from_metadata(&media_type, &file_name),
            ),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ATTACHMENT_FLAGS | PID_TAG_ATTACHMENT_LINK_ID => write_u32(&mut row, 0),
            PID_TAG_ATTACH_FLAGS => {
                write_u32(&mut row, if content_id.is_some() || hidden { 4 } else { 0 })
            }
            PID_TAG_ATTACHMENT_HIDDEN => {
                row.push(if content_id.is_some() || hidden { 1 } else { 0 })
            }
            PID_TAG_ATTACH_CONTENT_ID_W => {
                write_utf16z(&mut row, content_id.as_deref().unwrap_or(""))
            }
            PID_TAG_ATTACH_RENDERING => write_u16_prefixed_bytes(&mut row, &[]),
            PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => write_u64(&mut row, 0),
            PID_TAG_ATTACH_DATA_BINARY => write_u16_prefixed_bytes(&mut row, data),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn pending_attachment_content_id(properties: &HashMap<u32, MapiValue>) -> Option<String> {
    optional_pending_text_property(properties, &[PID_TAG_ATTACH_CONTENT_ID_W])
        .map(|value| value.trim().trim_matches(['<', '>']).to_string())
        .filter(|value| !value.is_empty())
}

fn pending_attachment_hidden(properties: &HashMap<u32, MapiValue>) -> bool {
    properties
        .get(&PID_TAG_ATTACHMENT_HIDDEN)
        .and_then(MapiValue::as_bool)
        .unwrap_or(false)
}

pub(in crate::mapi) fn serialize_saved_attachment_row(
    attach_num: u32,
    file_reference: &str,
    file_name: &str,
    media_type: &str,
    disposition: Option<&str>,
    content_id: Option<&str>,
    size_octets: u64,
    columns: &[u32],
) -> Vec<u8> {
    let is_inline = disposition.is_some_and(|value| value.eq_ignore_ascii_case("inline"))
        || content_id.is_some();
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_DISPLAY_NAME_W | PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, file_name)
            }
            PID_TAG_ATTACH_EXTENSION_W => {
                write_utf16z(&mut row, &attachment_file_extension(file_name))
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size_octets.min(u32::MAX as u64) as u32),
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, ATTACH_BY_VALUE),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ATTACHMENT_FLAGS | PID_TAG_ATTACHMENT_LINK_ID => write_u32(&mut row, 0),
            PID_TAG_ATTACH_FLAGS => write_u32(&mut row, if is_inline { 4 } else { 0 }),
            PID_TAG_ATTACHMENT_HIDDEN => row.push(if is_inline { 1 } else { 0 }),
            PID_TAG_ATTACH_CONTENT_ID_W => write_utf16z(&mut row, content_id.unwrap_or("")),
            PID_TAG_ATTACH_RENDERING => write_u16_prefixed_bytes(&mut row, &[]),
            PID_TAG_CREATION_TIME | PID_TAG_LAST_MODIFICATION_TIME => write_u64(&mut row, 0),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
                write_u16_prefixed_bytes(&mut row, file_reference.as_bytes())
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}
