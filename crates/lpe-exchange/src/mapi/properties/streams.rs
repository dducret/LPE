use super::*;

pub(in crate::mapi) async fn attachment_stream_data<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    input_handle: u32,
    open_mode: u8,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    match session.handles.get(&input_handle)?.clone() {
        MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        } if open_mode == 0 => {
            let attachment = snapshot.attachment_for_message(folder_id, message_id, attach_num)?;
            let content = store
                .fetch_attachment_content(principal.account_id, &attachment.file_reference)
                .await
                .ok()??;
            Some((content.blob_bytes, None))
        }
        MapiObject::PendingAttachment { data, .. } => match open_mode {
            0 => Some((data, None)),
            1 => Some((
                data,
                Some(StreamWriteTarget::PendingAttachment(input_handle)),
            )),
            2 => {
                if let Some(MapiObject::PendingAttachment { data, .. }) =
                    session.handles.get_mut(&input_handle)
                {
                    data.clear();
                }
                Some((
                    Vec::new(),
                    Some(StreamWriteTarget::PendingAttachment(input_handle)),
                ))
            }
            _ => None,
        },
        MapiObject::SavedAttachment { file_reference, .. } if open_mode == 0 => {
            let content = store
                .fetch_attachment_content(principal.account_id, &file_reference)
                .await
                .ok()??;
            Some((content.blob_bytes, None))
        }
        _ => None,
    }
}

pub(in crate::mapi) async fn open_stream_data<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    input_handle: u32,
    property_tag: u32,
    open_mode: u8,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    match property_tag {
        PID_TAG_ATTACH_DATA_BINARY => {
            attachment_stream_data(store, principal, session, input_handle, open_mode, snapshot)
                .await
        }
        PID_TAG_BODY_STRING8
        | PID_TAG_BODY_W
        | PID_TAG_RTF_COMPRESSED
        | PID_TAG_BODY_HTML_W
        | PID_TAG_HTML_BINARY => message_body_stream_data(
            session,
            input_handle,
            property_tag,
            open_mode,
            mailboxes,
            emails,
            snapshot,
        ),
        _ => property_stream_data(
            session,
            input_handle,
            property_tag,
            open_mode,
            mailboxes,
            principal.account_id,
            snapshot,
        ),
    }
}

pub(super) fn property_stream_data(
    session: &mut MapiSession,
    input_handle: u32,
    property_tag: u32,
    open_mode: u8,
    mailboxes: &[JmapMailbox],
    mailbox_guid: Uuid,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    let object = session.handles.get(&input_handle)?;
    let writable_associated_config = matches!(
        (object, open_mode),
        (MapiObject::AssociatedConfig { .. }, 1 | 2)
    );
    let writable_common_view_named_view =
        matches!(
            (object, open_mode),
            (MapiObject::CommonViewNamedView { .. }, 1 | 2)
        ) && common_view_named_view_stream_property_is_writable(property_tag);
    let writable_pending_event = matches!(
        (object, open_mode),
        (MapiObject::PendingEvent { .. }, 1 | 2)
    );
    if open_mode != 0
        && !writable_associated_config
        && !writable_common_view_named_view
        && !writable_pending_event
    {
        return None;
    }
    let allow_empty_missing_stream = !matches!(object, MapiObject::AssociatedConfig { .. });
    let value = match object {
        MapiObject::Folder {
            folder_id,
            properties,
        } => properties
            .get(&canonical_property_storage_tag(property_tag))
            .cloned()
            .or_else(|| {
                mailboxes
                    .iter()
                    .find(|mailbox| mapi_folder_id(mailbox) == *folder_id)
                    .and_then(|mailbox| {
                        mailbox_property_value_with_context_for_account(
                            mailbox,
                            mailboxes,
                            property_tag,
                            mailbox_guid,
                        )
                    })
            }),
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
        } => snapshot
            .associated_config_message_for_id(*config_id)
            .or_else(|| saved_message.clone())
            .filter(|message| message.folder_id == *folder_id)
            .and_then(|message| {
                associated_config_property_value_with_mailbox_guid(
                    &message,
                    mailbox_guid,
                    property_tag,
                )
            }),
        MapiObject::CommonViewNamedView { folder_id, view_id } => snapshot
            .named_view_message_for_folder_and_id(*folder_id, *view_id)
            .and_then(|message| {
                common_view_named_view_property_value(&message, mailbox_guid, property_tag)
            }),
        MapiObject::PendingEvent { properties, .. } => match open_mode {
            2 => None,
            _ => properties
                .get(&canonical_property_storage_tag(property_tag))
                .cloned(),
        },
        MapiObject::Event {
            folder_id,
            event_id,
        } if open_mode == 0 => snapshot
            .event_for_id(*folder_id, *event_id)
            .and_then(|event| {
                event_property_value_with_reminder(
                    &event.event,
                    event.id,
                    event.folder_id,
                    property_tag,
                    snapshot.reminder_for_source("calendar", event.canonical_id),
                )
            }),
        _ => return None,
    };
    let stream = match value {
        Some(value) => mapi_value_stream_bytes(property_tag, value)?,
        None if allow_empty_missing_stream || writable_associated_config => {
            empty_stream_bytes_for_property_tag(property_tag)?
        }
        None => return None,
    };
    let target = if writable_associated_config {
        Some(StreamWriteTarget::AssociatedConfigProperty {
            handle: input_handle,
            property_tag,
        })
    } else if writable_common_view_named_view {
        Some(StreamWriteTarget::VolatileProperty)
    } else if writable_pending_event {
        Some(StreamWriteTarget::PendingEventProperty {
            handle: input_handle,
            property_tag,
        })
    } else {
        None
    };
    Some((stream, target))
}

fn common_view_named_view_stream_property_is_writable(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_TAG_VIEW_DESCRIPTOR_BINARY
            | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
            | PID_TAG_VIEW_DESCRIPTOR_STRINGS_W
            | OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C
            | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
    )
}

fn mapi_value_stream_bytes(property_tag: u32, value: MapiValue) -> Option<Vec<u8>> {
    match value {
        MapiValue::Binary(value) => Some(value),
        MapiValue::String(value)
            if canonical_property_storage_tag(property_tag)
                == PID_TAG_VIEW_DESCRIPTOR_STRINGS_W =>
        {
            Some(utf16_bytes(&value))
        }
        MapiValue::String(value) if property_tag_type(property_tag) == 0x001E => {
            Some(string8z_bytes(&value))
        }
        MapiValue::String(value) => Some(utf16z_bytes(&value)),
        _ => None,
    }
}

fn empty_stream_bytes_for_property_tag(property_tag: u32) -> Option<Vec<u8>> {
    match property_tag_type(property_tag) {
        0x0102 => Some(Vec::new()),
        0x001E => Some(string8z_bytes("")),
        0x001F => Some(utf16z_bytes("")),
        _ => None,
    }
}

pub(super) fn property_tag_type(property_tag: u32) -> u32 {
    property_tag & 0x0000_FFFF
}

pub(in crate::mapi) fn message_body_stream_data(
    session: &MapiSession,
    input_handle: u32,
    property_tag: u32,
    open_mode: u8,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<StreamWriteTarget>)> {
    if property_tag == PID_TAG_RTF_COMPRESSED && open_mode != 0 {
        return None;
    }

    let (body_text, body_html) = match session.handles.get(&input_handle)? {
        MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        } if open_mode == 0 => {
            let email = message_for_id(*folder_id, *message_id, mailboxes, emails)
                .or(saved_email.as_ref().map(|saved| &saved.email))?;
            (email.body_text.clone(), email.body_html_sanitized.clone())
        }
        MapiObject::PendingMessage { properties, .. }
        | MapiObject::PendingAssociatedMessage { properties, .. } => match open_mode {
            0 | 1 => (
                pending_text_property(properties, &[PID_TAG_BODY_W]),
                optional_pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
                    .or_else(|| pending_html_binary_property(properties)),
            ),
            2 => (String::new(), Some(String::new())),
            _ => return None,
        },
        MapiObject::PendingEvent { properties, .. } => match open_mode {
            0 | 1 => (
                pending_text_property(properties, &[PID_TAG_BODY_W]),
                optional_pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
                    .or_else(|| pending_html_binary_property(properties)),
            ),
            2 => (String::new(), Some(String::new())),
            _ => return None,
        },
        MapiObject::PublicFolderItem {
            folder_id,
            item_id,
            properties,
        } => match open_mode {
            0 | 1 => {
                let item = snapshot.public_folder_item_for_id(*folder_id, *item_id)?;
                (
                    optional_pending_text_property(properties, &[PID_TAG_BODY_W])
                        .unwrap_or_else(|| item.item.body_text.clone()),
                    optional_pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
                        .or_else(|| pending_html_binary_property(properties))
                        .or_else(|| item.item.body_html_sanitized.clone()),
                )
            }
            2 => (String::new(), Some(String::new())),
            _ => return None,
        },
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
        } if open_mode == 0 => {
            let message = snapshot
                .associated_config_message_for_id(*config_id)
                .or_else(|| saved_message.clone())
                .filter(|message| message.folder_id == *folder_id)?;
            let body_text = match associated_config_property_value(&message, PID_TAG_BODY_W) {
                Some(MapiValue::String(value)) => value,
                _ => String::new(),
            };
            let body_html = match associated_config_property_value(&message, PID_TAG_BODY_HTML_W) {
                Some(MapiValue::String(value)) => Some(value),
                _ => match associated_config_property_value(&message, PID_TAG_HTML_BINARY) {
                    Some(MapiValue::Binary(value)) => String::from_utf8(value).ok(),
                    Some(MapiValue::String(value)) => Some(value),
                    _ => None,
                },
            };
            (body_text, body_html)
        }
        _ => return None,
    };

    let body_html = body_html.or_else(|| html_body_from_plain_text(&body_text));
    let stream = match (property_tag, open_mode) {
        (_, 2) => Vec::new(),
        (PID_TAG_BODY_STRING8, _) => string8z_bytes(&body_text),
        (PID_TAG_BODY_W, _) => utf16z_bytes(&body_text),
        (PID_TAG_RTF_COMPRESSED, _) => uncompressed_rtf_body(&body_text),
        (PID_TAG_BODY_HTML_W, _) => utf16z_bytes(body_html.as_deref().unwrap_or("")),
        (PID_TAG_HTML_BINARY, _) => body_html.unwrap_or_default().into_bytes(),
        _ => return None,
    };
    let target = match (session.handles.get(&input_handle), open_mode) {
        (Some(MapiObject::PendingMessage { .. }), 1 | 2) => {
            Some(StreamWriteTarget::PendingMessageProperty {
                handle: input_handle,
                property_tag,
            })
        }
        (Some(MapiObject::PendingEvent { .. }), 1 | 2) => {
            Some(StreamWriteTarget::PendingEventProperty {
                handle: input_handle,
                property_tag,
            })
        }
        (Some(MapiObject::PendingAssociatedMessage { .. }), 1 | 2) => {
            Some(StreamWriteTarget::PendingAssociatedMessageProperty {
                handle: input_handle,
                property_tag,
            })
        }
        (Some(MapiObject::PublicFolderItem { .. }), 1 | 2) => {
            Some(StreamWriteTarget::PublicFolderItemProperty {
                handle: input_handle,
                property_tag,
            })
        }
        _ => None,
    };
    Some((stream, target))
}

pub(in crate::mapi) fn utf16z_bytes(value: &str) -> Vec<u8> {
    let mut bytes = value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes
}

fn utf16_bytes(value: &str) -> Vec<u8> {
    value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>()
}

pub(in crate::mapi) fn string8z_bytes(value: &str) -> Vec<u8> {
    let mut bytes = value
        .bytes()
        .map(|byte| if byte.is_ascii() { byte } else { b'?' })
        .collect::<Vec<_>>();
    bytes.push(0);
    bytes
}

pub(in crate::mapi) fn pending_html_binary_property(
    properties: &HashMap<u32, MapiValue>,
) -> Option<String> {
    properties
        .get(&PID_TAG_HTML_BINARY)
        .and_then(|value| match value {
            MapiValue::Binary(bytes) => String::from_utf8(bytes.clone()).ok(),
            MapiValue::String(value) => Some(value.clone()),
            _ => None,
        })
}

pub(in crate::mapi) fn pending_html_property(
    properties: &HashMap<u32, MapiValue>,
) -> Option<String> {
    optional_pending_text_property(properties, &[PID_TAG_BODY_HTML_W])
        .or_else(|| pending_html_binary_property(properties))
        .filter(|value| !value.trim().is_empty())
}

pub(in crate::mapi) fn write_stream(
    session: &mut MapiSession,
    stream_handle: u32,
    bytes: &[u8],
) -> Option<usize> {
    let (updated_data, writable_target, written) = {
        let Some(MapiObject::AttachmentStream {
            data,
            position,
            writable_target: Some(writable_target),
        }) = session.handles.get_mut(&stream_handle)
        else {
            return None;
        };
        let start = *position;
        let end = start.checked_add(bytes.len())?;
        if data.len() < end {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(bytes);
        *position = end;
        (data.clone(), *writable_target, bytes.len())
    };

    sync_stream_target(session, writable_target, updated_data)?;
    Some(written)
}

pub(in crate::mapi) fn resolve_writable_stream_handle(
    session: &MapiSession,
    requested_handle: u32,
) -> Option<u32> {
    if matches!(
        session.handles.get(&requested_handle),
        Some(MapiObject::AttachmentStream { .. })
    ) {
        return Some(requested_handle);
    }
    if !matches!(
        session.handles.get(&requested_handle),
        Some(MapiObject::AssociatedConfig { .. })
    ) {
        return None;
    }

    let mut matches = session
        .handles
        .iter()
        .filter_map(|(handle, object)| match object {
            MapiObject::AttachmentStream {
                writable_target:
                    Some(StreamWriteTarget::AssociatedConfigProperty {
                        handle: target_handle,
                        ..
                    }),
                ..
            } if *target_handle == requested_handle => Some(*handle),
            _ => None,
        });
    let handle = matches.next()?;
    matches.next().is_none().then_some(handle)
}

pub(in crate::mapi) fn stream_write_error(
    session: &MapiSession,
    stream_handle: u32,
) -> Option<StreamWriteError> {
    match session.handles.get(&stream_handle) {
        Some(MapiObject::AttachmentStream {
            writable_target: None,
            ..
        }) => Some(StreamWriteError::AccessDenied),
        Some(MapiObject::AttachmentStream { .. }) => None,
        _ => Some(StreamWriteError::NotFound),
    }
}

pub(in crate::mapi) fn stream_write_error_code(error: StreamWriteError) -> u32 {
    match error {
        StreamWriteError::NotFound => 0x8004_010F,
        StreamWriteError::AccessDenied => 0x8003_0005,
    }
}

pub(in crate::mapi) fn copy_stream(
    session: &mut MapiSession,
    source_handle: u32,
    destination_handle: u32,
    byte_count: u64,
) -> Option<(usize, usize)> {
    let requested = usize::try_from(byte_count).ok()?;
    let chunk = {
        let Some(MapiObject::AttachmentStream { data, position, .. }) =
            session.handles.get_mut(&source_handle)
        else {
            return None;
        };
        let end = position.saturating_add(requested).min(data.len());
        let chunk = data[*position..end].to_vec();
        *position = end;
        chunk
    };
    let written = write_stream(session, destination_handle, &chunk)?;
    Some((chunk.len(), written))
}

pub(in crate::mapi) fn sync_stream_target(
    session: &mut MapiSession,
    target: StreamWriteTarget,
    data: Vec<u8>,
) -> Option<()> {
    match target {
        StreamWriteTarget::PendingAttachment(handle) => {
            if let Some(MapiObject::PendingAttachment {
                data: attachment_data,
                ..
            }) = session.handles.get_mut(&handle)
            {
                *attachment_data = data;
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::PendingMessageProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::PendingMessage { properties, .. }) =
                session.handles.get_mut(&handle)
            {
                properties.insert(canonical_property_storage_tag(property_tag), value);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::PendingEventProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::PendingEvent { properties, .. }) =
                session.handles.get_mut(&handle)
            {
                properties.insert(canonical_property_storage_tag(property_tag), value);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::PendingAssociatedMessageProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::PendingAssociatedMessage { properties, .. }) =
                session.handles.get_mut(&handle)
            {
                properties.insert(canonical_property_storage_tag(property_tag), value);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::AssociatedConfigProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::AssociatedConfig {
                saved_message: Some(message),
                ..
            }) = session.handles.get_mut(&handle)
            {
                let mut properties = mapi_properties_from_json(&message.properties_json);
                properties.insert(canonical_property_storage_tag(property_tag), value);
                message.properties_json = mapi_properties_to_json(&properties);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::PublicFolderItemProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::PublicFolderItem { properties, .. }) =
                session.handles.get_mut(&handle)
            {
                properties.insert(canonical_property_storage_tag(property_tag), value);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::VolatileProperty => Some(()),
    }
}

pub(in crate::mapi) fn stream_property_value(
    property_tag: u32,
    data: Vec<u8>,
) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_RTF_COMPRESSED => None,
        PID_TAG_BODY_STRING8 => Some(MapiValue::String(decode_string8_stream_value(&data))),
        PID_TAG_BODY_W | PID_TAG_BODY_HTML_W => {
            Some(MapiValue::String(decode_utf16_stream_value(&data)?))
        }
        PID_TAG_HTML_BINARY => Some(MapiValue::Binary(data)),
        _ if property_tag_type(property_tag) == 0x0102 => Some(MapiValue::Binary(data)),
        _ => None,
    }
}

pub(in crate::mapi) fn decode_string8_stream_value(data: &[u8]) -> String {
    let value = data
        .strip_suffix(&[0])
        .or_else(|| data.strip_suffix(&[0, 0]))
        .unwrap_or(data);
    String::from_utf8_lossy(value).into_owned()
}

pub(in crate::mapi) fn decode_utf16_stream_value(data: &[u8]) -> Option<String> {
    let even_len = data.len() - (data.len() % 2);
    let mut units = data[..even_len]
        .chunks_exact(2)
        .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
        .collect::<Vec<_>>();
    if units.last().is_some_and(|unit| *unit == 0) {
        units.pop();
    }
    String::from_utf16(&units).ok()
}

pub(in crate::mapi) fn set_attachment_stream_size(
    session: &mut MapiSession,
    stream_handle: u32,
    stream_size: u64,
) -> Option<()> {
    let requested_size = usize::try_from(stream_size).ok()?;
    if requested_size > i32::MAX as usize {
        return None;
    }

    let (updated_data, writable_target) = {
        let Some(MapiObject::AttachmentStream {
            data,
            position,
            writable_target: Some(writable_target),
        }) = session.handles.get_mut(&stream_handle)
        else {
            return None;
        };
        data.resize(requested_size, 0);
        *position = (*position).min(data.len());
        (data.clone(), *writable_target)
    };

    sync_stream_target(session, writable_target, updated_data)
}

pub(in crate::mapi) fn pending_message_size(properties: &HashMap<u32, MapiValue>) -> i64 {
    let subject = pending_text_property(
        properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    );
    let body = pending_body_text_property(properties);
    subject
        .len()
        .saturating_add(body.len())
        .min(i64::MAX as usize) as i64
}

pub(super) fn pending_body_text_property(properties: &HashMap<u32, MapiValue>) -> String {
    let body_text = pending_text_property(properties, &[PID_TAG_BODY_W]);
    if !body_text.trim().is_empty() {
        return body_text;
    }
    pending_html_property(properties)
        .map(|value| plain_text_from_html_body(&value))
        .unwrap_or_default()
}

pub(in crate::mapi) fn pending_text_property(
    properties: &HashMap<u32, MapiValue>,
    tags: &[u32],
) -> String {
    tags.iter()
        .find_map(|tag| {
            properties
                .get(tag)
                .and_then(|value| value.clone().into_text())
        })
        .unwrap_or_default()
}

pub(in crate::mapi) fn optional_pending_text_property(
    properties: &HashMap<u32, MapiValue>,
    tags: &[u32],
) -> Option<String> {
    tags.iter()
        .find_map(|tag| {
            properties
                .get(tag)
                .and_then(|value| value.clone().into_text())
        })
        .filter(|value| !value.trim().is_empty())
}

fn plain_text_from_html_body(html: &str) -> String {
    let mut text = String::new();
    let mut tag = String::new();
    let mut in_tag = false;
    for ch in html.chars() {
        match (in_tag, ch) {
            (false, '<') => {
                in_tag = true;
                tag.clear();
            }
            (true, '>') => {
                in_tag = false;
                if html_tag_is_line_break(&tag) && !text.ends_with('\n') {
                    text.push('\n');
                }
            }
            (true, _) => tag.push(ch),
            (false, _) => text.push(ch),
        }
    }
    decode_basic_html_entities(&text)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn html_tag_is_line_break(tag: &str) -> bool {
    let tag_name = tag
        .trim()
        .trim_start_matches('/')
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_ascii_lowercase();
    matches!(tag_name.as_str(), "br" | "p" | "div" | "li")
}

fn decode_basic_html_entities(value: &str) -> String {
    value
        .replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session(object: MapiObject) -> MapiSession {
        let mut handles = HashMap::new();
        handles.insert(1, object);
        MapiSession {
            endpoint: MapiEndpoint::Emsmdb,
            tenant_id: Uuid::nil(),
            account_id: Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376),
            email: "sender@example.test".to_string(),
            created_at: SystemTime::UNIX_EPOCH,
            last_seen_at: SystemTime::UNIX_EPOCH,
            first_request_type: String::new(),
            first_request_id: String::new(),
            last_request_type: String::new(),
            last_request_id: String::new(),
            request_count: 0,
            execute_request_count: 0,
            next_handle: 2,
            handles,
            message_statuses: HashMap::new(),
            message_save_generations: HashMap::new(),
            message_handle_generations: HashMap::new(),
            pending_message_recipient_replacements: HashMap::new(),
            pending_message_attachments: HashMap::new(),
            pending_attachment_parent_messages: HashMap::new(),
            pending_attachment_deletions: HashSet::new(),
            pending_embedded_message_ids: HashMap::new(),
            pending_embedded_message_attachments: HashMap::new(),
            saved_embedded_messages: HashMap::new(),
            saved_search_folder_definitions: HashMap::new(),
            special_folder_aliases: HashMap::new(),
            deleted_advertised_special_folders: HashSet::new(),
            deleted_search_folder_definitions: HashSet::new(),
            named_properties: HashMap::new(),
            named_property_ids: HashMap::new(),
            next_named_property_id: FIRST_NAMED_PROPERTY_ID,
            next_local_replica_sequence: 1,
            notification_cursor: None,
            pending_notifications: VecDeque::new(),
            table_notification_eligible_handles: HashSet::new(),
            table_notification_active_handles: HashSet::new(),
            completed_execute_requests: HashMap::new(),
            completed_execute_request_order: VecDeque::new(),
            post_hierarchy_actions: PostHierarchyActionState::default(),
            default_view_advertisements: HashMap::new(),
            inbox_associated_config_stream_handles: HashSet::new(),
            inbox_rule_organizer_stream_handles: HashSet::new(),
            logon_identity: None,
            outlook_smart_input_variant: "none".to_string(),
            outlook_smart_input_variant_applied: false,
        }
    }

    #[test]
    fn rtf_compressed_body_stream_is_read_only_projection() {
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_BODY_W,
            MapiValue::String("Canonical body".to_string()),
        );
        let object = MapiObject::PendingMessage {
            folder_id: DRAFTS_FOLDER_ID,
            properties,
            recipients: Vec::new(),
        };
        let session = test_session(object);
        let snapshot = MapiMailStoreSnapshot::empty();

        let (stream, writable_target) =
            message_body_stream_data(&session, 1, PID_TAG_RTF_COMPRESSED, 0, &[], &[], &snapshot)
                .expect("readable synthesized RTF stream");
        assert!(writable_target.is_none());
        assert_eq!(
            u32::from_le_bytes(stream[8..12].try_into().unwrap()),
            0x414C_454D
        );
        assert!(String::from_utf8_lossy(&stream[16..]).contains("Canonical body"));

        assert!(message_body_stream_data(
            &session,
            1,
            PID_TAG_RTF_COMPRESSED,
            1,
            &[],
            &[],
            &snapshot,
        )
        .is_none());
        assert!(message_body_stream_data(
            &session,
            1,
            PID_TAG_RTF_COMPRESSED,
            2,
            &[],
            &[],
            &snapshot,
        )
        .is_none());
    }

    #[test]
    fn stream_property_value_rejects_client_originated_rtf_bytes() {
        assert_eq!(
            stream_property_value(PID_TAG_RTF_COMPRESSED, b"opaque rtf".to_vec()),
            None
        );
    }
}
