use super::*;

mod calendar;
use calendar::*;

// [MS-OXOCAL] sections 2.2.12.5 and 2.2.12.5.1. This is the only
// appointment-tombstone value that has no canonical deleted-meeting state.
pub(in crate::mapi) const EMPTY_APPOINTMENT_TOMBSTONE: [u8; 20] = [
    0xCD, 0xAF, 0xDE, 0xBE, // Identifier = 0xBEDEAFCD.
    0x14, 0x00, 0x00, 0x00, // HeaderSize = 0x14.
    0x03, 0x00, 0x00, 0x00, // Version = 3.
    0x00, 0x00, 0x00, 0x00, // RecordsCount = 0.
    0x14, 0x00, 0x00, 0x00, // RecordsSize = 0x14.
];

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
    // The Outlook compatibility exception is limited to imported SetProperties;
    // SearchKey remains read-only through the generic property-stream surface.
    let writable_pending_event = matches!(
        (object, open_mode),
        (MapiObject::PendingEvent { .. }, 1 | 2)
    ) && calendar_event_stream_property_is_writable(property_tag);
    let writable_event = match (object, open_mode) {
        (
            MapiObject::Event {
                folder_id,
                event_id,
                transaction,
            },
            1 | 2,
        ) if transaction.import_disposition == MapiEventImportDisposition::Apply
            && matches!(transaction.open_mode_flags & 0x03, 0x01 | 0x03)
            && calendar_event_stream_property_is_writable(property_tag) =>
        {
            snapshot
                .event_for_id(*folder_id, *event_id)
                .is_some_and(|event| event.event.rights.may_write)
        }
        _ => false,
    };
    let writable_pending_associated_message = matches!(
        (object, open_mode),
        (
            MapiObject::PendingAssociatedMessage { .. }
                | MapiObject::PendingNavigationShortcut { .. },
            1 | 2
        )
    );
    let writable_local_freebusy_tombstone = matches!(
        (object, open_mode),
        (
            MapiObject::DelegateFreeBusyMessage { message_id, .. },
            1 | 2
        ) if crate::mapi_store::is_outlook_local_freebusy_message_id(*message_id)
            && canonical_property_storage_tag(property_tag)
                == PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE
    );
    if open_mode != 0
        && !writable_associated_config
        && !writable_common_view_named_view
        && !writable_pending_event
        && !writable_event
        && !writable_pending_associated_message
        && !writable_local_freebusy_tombstone
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
        } => saved_message
            .clone()
            .or_else(|| snapshot.associated_config_message_for_id(*config_id))
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
                .get(&canonical_calendar_property_storage_tag(property_tag))
                .cloned(),
        },
        MapiObject::PendingAssociatedMessage { properties, .. }
        | MapiObject::PendingNavigationShortcut { properties, .. } => match open_mode {
            2 => None,
            _ => properties
                .get(&canonical_property_storage_tag(property_tag))
                .cloned(),
        },
        MapiObject::DelegateFreeBusyMessage {
            message_id,
            pending_appointment_tombstone,
            ..
        } if crate::mapi_store::is_outlook_local_freebusy_message_id(*message_id)
            && canonical_property_storage_tag(property_tag)
                == PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE =>
        {
            (open_mode != 2).then(|| {
                MapiValue::Binary(
                    pending_appointment_tombstone
                        .clone()
                        .unwrap_or_else(|| EMPTY_APPOINTMENT_TOMBSTONE.to_vec()),
                )
            })
        }
        MapiObject::Event {
            folder_id,
            event_id,
            transaction,
        } => snapshot
            .event_for_id(*folder_id, *event_id)
            .and_then(|event| {
                (open_mode != 2).then_some(())?;
                effective_event_stream_property_value(
                    event,
                    transaction,
                    property_tag,
                    mailbox_guid,
                    snapshot,
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
    } else if writable_event {
        Some(StreamWriteTarget::EventProperty {
            handle: input_handle,
            property_tag,
        })
    } else if writable_pending_associated_message {
        // [MS-OXOCFG] section 2.2.6 stores view definitions in stream
        // properties on an FAI Message. A new Common Views message remains a
        // PendingNavigationShortcut until SaveChangesMessage classifies it.
        Some(StreamWriteTarget::PendingAssociatedMessageProperty {
            handle: input_handle,
            property_tag,
        })
    } else if writable_local_freebusy_tombstone {
        // [MS-OXOCAL] sections 2.2.12.5 and 2.2.12.5.1 define this optional
        // client-maintained tombstone stream on delegate information. LPE's
        // LocalFreebusy object is computed from canonical calendar state. The
        // stream remains transactional until SaveChangesMessage validates
        // that it contains the empty structure and no deleted-meeting state.
        Some(StreamWriteTarget::DelegateFreeBusyAppointmentTombstone {
            handle: input_handle,
        })
    } else {
        None
    };
    if open_mode == 2 {
        if let Some(StreamWriteTarget::DelegateFreeBusyAppointmentTombstone { handle }) = target {
            let MapiObject::DelegateFreeBusyMessage {
                pending_appointment_tombstone,
                ..
            } = session.handles.get_mut(&handle)?
            else {
                return None;
            };
            // Create truncates the property. Record that zero-length staged
            // value so SaveChangesMessage rejects it unless subsequent stream
            // writes produce the complete valid empty structure.
            *pending_appointment_tombstone = Some(Vec::new());
        }
    }
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
        MapiObject::Event {
            folder_id,
            event_id,
            transaction,
        } => {
            let event = snapshot.event_for_id(*folder_id, *event_id)?;
            if open_mode != 0
                && (!matches!(transaction.open_mode_flags & 0x03, 0x01 | 0x03)
                    || !event.event.rights.may_write
                    || transaction.import_disposition != MapiEventImportDisposition::Apply)
            {
                return None;
            }
            match open_mode {
                0 | 1 => {
                    let body_text = if transaction.deleted_properties.contains(&PID_TAG_BODY_W) {
                        String::new()
                    } else {
                        optional_pending_text_property(
                            &transaction.pending_properties,
                            &[PID_TAG_BODY_W],
                        )
                        .unwrap_or_else(|| event.event.notes.clone())
                    };
                    let body_html = optional_pending_text_property(
                        &transaction.pending_properties,
                        &[PID_TAG_BODY_HTML_W],
                    )
                    .or_else(|| pending_html_binary_property(&transaction.pending_properties))
                    .or_else(|| {
                        (!transaction
                            .deleted_properties
                            .contains(&PID_TAG_BODY_HTML_W)
                            && !transaction
                                .deleted_properties
                                .contains(&PID_TAG_HTML_BINARY))
                        .then(|| event.event.body_html.clone())
                    });
                    (body_text, body_html)
                }
                2 => (String::new(), Some(String::new())),
                _ => return None,
            }
        }
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
            let message = saved_message
                .clone()
                .or_else(|| snapshot.associated_config_message_for_id(*config_id))
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
        (Some(MapiObject::PendingEvent { .. }), 1 | 2)
            if calendar_event_stream_property_is_writable(property_tag) =>
        {
            Some(StreamWriteTarget::PendingEventProperty {
                handle: input_handle,
                property_tag,
            })
        }
        (Some(MapiObject::Event { .. }), 1 | 2) => Some(StreamWriteTarget::EventProperty {
            handle: input_handle,
            property_tag,
        }),
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

pub(in crate::mapi) fn exact_attachment_stream_handle(
    session: &MapiSession,
    requested_handle: u32,
) -> Option<u32> {
    matches!(
        session.handles.get(&requested_handle),
        Some(MapiObject::AttachmentStream { .. })
    )
    .then_some(requested_handle)
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
                insert_pending_event_stream_property(properties, property_tag, value);
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::EventProperty {
            handle,
            property_tag,
        } => {
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::Event { transaction, .. }) = session.handles.get_mut(&handle) {
                insert_event_stream_property(transaction, property_tag, value);
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
            match session.handles.get_mut(&handle) {
                Some(MapiObject::PendingAssociatedMessage { properties, .. })
                | Some(MapiObject::PendingNavigationShortcut { properties, .. }) => {
                    let property_tag = canonical_property_storage_tag(property_tag);
                    if !crate::mapi_store::is_associated_config_read_only_property_tag(property_tag)
                    {
                        properties.insert(property_tag, value);
                    }
                    Some(())
                }
                _ => None,
            }
        }
        StreamWriteTarget::AssociatedConfigProperty {
            handle,
            property_tag,
        } => {
            let property_tag = canonical_property_storage_tag(property_tag);
            if crate::mapi_store::is_associated_config_read_only_property_tag(property_tag)
                || property_tag == PID_TAG_SEARCH_KEY
            {
                return Some(());
            }
            let value = stream_property_value(property_tag, data)?;
            if let Some(MapiObject::AssociatedConfig {
                saved_message: Some(message),
                ..
            }) = session.handles.get_mut(&handle)
            {
                let mut properties = mapi_properties_from_json(&message.properties_json);
                properties.insert(property_tag, value);
                let mut properties_json = mapi_properties_to_json(&properties);
                crate::mapi_store::copy_associated_config_server_metadata(
                    &message.properties_json,
                    &mut properties_json,
                );
                message.properties_json = properties_json;
                Some(())
            } else {
                None
            }
        }
        StreamWriteTarget::DelegateFreeBusyAppointmentTombstone { handle } => {
            if let Some(MapiObject::DelegateFreeBusyMessage {
                pending_appointment_tombstone,
                ..
            }) = session.handles.get_mut(&handle)
            {
                *pending_appointment_tombstone = Some(data);
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
        _ if property_tag_type(property_tag) == 0x001E => {
            Some(MapiValue::String(decode_string8_stream_value(&data)))
        }
        _ if property_tag_type(property_tag) == 0x001F => {
            Some(MapiValue::String(decode_utf16_stream_value(&data)?))
        }
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
    if let Some(html) = pending_html_property(properties) {
        return plain_text_from_html_body(&html);
    }
    match properties.get(&PID_TAG_RTF_COMPRESSED) {
        Some(MapiValue::Binary(value)) => {
            super::rtf::plain_text_from_rtf_container(value).unwrap_or_default()
        }
        _ => String::new(),
    }
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

    fn pending_event_session() -> MapiSession {
        test_session(MapiObject::PendingEvent {
            folder_id: CALENDAR_FOLDER_ID,
            properties: HashMap::new(),
            recipients: Vec::new(),
            recipients_modified: false,
            fail_on_conflict: false,
        })
    }

    fn saved_event_session_and_snapshot() -> (MapiSession, MapiMailStoreSnapshot) {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let canonical_id = Uuid::from_u128(0x9100);
        let event_id = crate::mapi::identity::mapi_store_id(0x9100);
        let mut snapshot = MapiMailStoreSnapshot::empty();
        snapshot.remember_created_event(
            CALENDAR_FOLDER_ID,
            event_id,
            lpe_storage::AccessibleEvent {
                id: canonical_id,
                uid: "stream-event".to_string(),
                collection_id: "default".to_string(),
                owner_account_id: account_id,
                owner_email: "sender@example.test".to_string(),
                owner_display_name: "Sender".to_string(),
                rights: lpe_storage::CollaborationRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: false,
                },
                date: "2026-08-12".to_string(),
                time: "09:00".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 30,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 0,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "Stream event".to_string(),
                location: String::new(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: "[]".to_string(),
                notes: "Body".to_string(),
                body_html: "<p>Body</p>".to_string(),
            },
            Vec::new(),
        );
        let stored_values = [
            (0x9100_0102, MapiValue::Binary(b"persisted binary".to_vec())),
            (
                0x9101_001F,
                MapiValue::String("persisted unicode".to_string()),
            ),
        ]
        .into_iter()
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, property_tag, &value);
            crate::store::MapiCalendarPropertyValue {
                event_id: canonical_id,
                property_tag,
                property_type: property_tag as u16,
                property_value,
            }
        })
        .collect();
        let snapshot = snapshot.with_calendar_property_values(stored_values);
        let session = test_session(MapiObject::Event {
            folder_id: CALENDAR_FOLDER_ID,
            event_id,
            transaction: MapiEventTransaction::new(1, 1),
        });
        (session, snapshot)
    }

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
            request_sequence_token: "test-sequence".to_string(),
            request_count: 0,
            execute_request_count: 0,
            next_handle: 2,
            handles,
            issued_handles: HashSet::new(),
            folder_profile_property_tombstones: HashMap::new(),
            message_statuses: HashMap::new(),
            message_save_generations: HashMap::new(),
            message_handle_generations: HashMap::new(),
            pending_message_recipient_replacements: HashMap::new(),
            pending_message_attachments: HashMap::new(),
            pending_sync_import_source_keys: HashMap::new(),
            pending_attachment_parent_messages: HashMap::new(),
            pending_event_attachment_transactions: HashMap::new(),
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
            notification_cursor: None,
            pending_notifications: VecDeque::new(),
            table_notification_eligible_handles: HashMap::new(),
            table_notification_active_handles: HashSet::new(),
            completed_execute_requests: HashMap::new(),
            completed_execute_request_order: VecDeque::new(),
            post_hierarchy_actions: PostHierarchyActionState::default(),
            default_view_advertisements: HashMap::new(),
            inbox_associated_config_stream_handles: HashSet::new(),
            inbox_rule_organizer_stream_handles: HashSet::new(),
            logon_identity: None,
            store_replica_guid: None,
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

    #[test]
    fn pending_calendar_stream_rejects_identity_and_wrong_type_canonical_properties() {
        let mut session = pending_event_session();
        let account_id = session.account_id;
        let snapshot = MapiMailStoreSnapshot::empty();
        let location_binary = (PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x0102;

        for property_tag in [PID_TAG_ENTRY_ID, location_binary] {
            assert!(property_stream_data(
                &mut session,
                1,
                property_tag,
                1,
                &[],
                account_id,
                &snapshot,
            )
            .is_none());
        }
    }

    #[test]
    fn calendar_stream_guard_rejects_all_server_managed_identity_type_variants() {
        for managed_tag in [
            PID_TAG_ENTRY_ID,
            PID_TAG_PARENT_ENTRY_ID,
            PID_TAG_INSTANCE_KEY,
            PID_TAG_RECORD_KEY,
            PID_TAG_SOURCE_KEY,
            PID_TAG_PARENT_SOURCE_KEY,
            PID_TAG_SEARCH_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
        ] {
            assert!(!calendar_event_stream_property_is_writable(managed_tag));
            assert!(!calendar_event_stream_property_is_writable(
                (managed_tag & 0xFFFF_0000) | 0x001F
            ));
        }
        assert!(!calendar_event_stream_property_is_writable(
            (PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x0102
        ));
        assert!(!calendar_event_stream_property_is_writable(
            (PID_TAG_SUBJECT_W & 0xFFFF_0000) | 0x0102
        ));
        assert!(!calendar_event_stream_property_is_writable(
            PID_TAG_SENDER_ENTRY_ID
        ));
        assert!(!calendar_event_stream_property_is_writable(
            PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W
        ));
        assert!(calendar_event_stream_property_is_writable(
            PID_TAG_SENDER_EMAIL_ADDRESS_W
        ));
        assert!(calendar_event_stream_property_is_writable(0x9100_0102));
        assert!(calendar_event_stream_property_is_writable(0x9101_001F));
        assert!(calendar_event_stream_property_is_writable(
            PID_TAG_HTML_BINARY
        ));
        assert!(calendar_event_stream_property_is_writable(
            PID_LID_APPOINTMENT_RECUR_TAG
        ));
    }

    #[test]
    fn pending_calendar_stream_commits_supported_named_properties_canonically() {
        let mut session = pending_event_session();
        let account_id = session.account_id;
        let snapshot = MapiMailStoreSnapshot::empty();
        let named_binary = 0x9100_0102;
        let named_string8 = 0x9101_001E;
        let named_unicode = 0x9102_001F;

        for (property_tag, data) in [
            (named_binary, b"named-binary".to_vec()),
            (named_string8, b"named string8\0".to_vec()),
            (named_unicode, utf16z_bytes("named unicode")),
        ] {
            let (_, target) =
                property_stream_data(&mut session, 1, property_tag, 2, &[], account_id, &snapshot)
                    .expect("supported named Calendar stream");
            sync_stream_target(
                &mut session,
                target.expect("writable named Calendar stream"),
                data,
            )
            .expect("commit named Calendar stream");
        }

        let MapiObject::PendingEvent { properties, .. } = session.handles.get(&1).unwrap() else {
            panic!("pending Calendar event");
        };
        assert_eq!(
            properties.get(&named_binary),
            Some(&MapiValue::Binary(b"named-binary".to_vec()))
        );
        assert!(!properties.contains_key(&named_string8));
        assert_eq!(
            properties.get(&0x9101_001F),
            Some(&MapiValue::String("named string8".to_string()))
        );
        assert_eq!(
            properties.get(&named_unicode),
            Some(&MapiValue::String("named unicode".to_string()))
        );
    }

    #[test]
    fn calendar_stream_pending_html_keeps_unicode_and_binary_aliases_coherent() {
        let mut session = pending_event_session();
        let snapshot = MapiMailStoreSnapshot::empty();

        let (_, target) =
            message_body_stream_data(&session, 1, PID_TAG_BODY_HTML_W, 2, &[], &[], &snapshot)
                .expect("writable Unicode HTML stream");
        sync_stream_target(
            &mut session,
            target.expect("Unicode HTML write target"),
            utf16z_bytes("<b>wide</b>"),
        )
        .expect("commit Unicode HTML stream");

        let MapiObject::PendingEvent { properties, .. } = session.handles.get(&1).unwrap() else {
            panic!("pending Calendar event");
        };
        assert_eq!(
            properties.get(&PID_TAG_BODY_HTML_W),
            Some(&MapiValue::String("<b>wide</b>".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_HTML_BINARY),
            Some(&MapiValue::Binary(b"<b>wide</b>".to_vec()))
        );

        let (_, target) =
            message_body_stream_data(&session, 1, PID_TAG_HTML_BINARY, 2, &[], &[], &snapshot)
                .expect("writable binary HTML stream");
        sync_stream_target(
            &mut session,
            target.expect("binary HTML write target"),
            b"<i>binary</i>".to_vec(),
        )
        .expect("commit binary HTML stream");

        let MapiObject::PendingEvent { properties, .. } = session.handles.get(&1).unwrap() else {
            panic!("pending Calendar event");
        };
        assert_eq!(
            properties.get(&PID_TAG_BODY_HTML_W),
            Some(&MapiValue::String("<i>binary</i>".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_HTML_BINARY),
            Some(&MapiValue::Binary(b"<i>binary</i>".to_vec()))
        );
    }

    #[test]
    fn calendar_stream_subject_keeps_subject_aliases_coherent() {
        let mut pending = pending_event_session();
        sync_stream_target(
            &mut pending,
            StreamWriteTarget::PendingEventProperty {
                handle: 1,
                property_tag: PID_TAG_NORMALIZED_SUBJECT_W,
            },
            utf16z_bytes("Pending subject"),
        )
        .expect("stage pending Event subject stream");
        let MapiObject::PendingEvent { properties, .. } = pending.handles.get(&1).unwrap() else {
            panic!("pending Calendar event");
        };
        assert_eq!(
            properties.get(&PID_TAG_SUBJECT_W),
            Some(&MapiValue::String("Pending subject".to_string()))
        );
        assert_eq!(
            properties.get(&PID_TAG_NORMALIZED_SUBJECT_W),
            Some(&MapiValue::String("Pending subject".to_string()))
        );

        let (mut saved, _) = saved_event_session_and_snapshot();
        sync_stream_target(
            &mut saved,
            StreamWriteTarget::EventProperty {
                handle: 1,
                property_tag: PID_TAG_SUBJECT_W,
            },
            utf16z_bytes("Saved subject"),
        )
        .expect("stage saved Event subject stream");
        let MapiObject::Event { transaction, .. } = saved.handles.get(&1).unwrap() else {
            panic!("saved Calendar event");
        };
        assert_eq!(
            transaction.pending_properties.get(&PID_TAG_SUBJECT_W),
            Some(&MapiValue::String("Saved subject".to_string()))
        );
        assert_eq!(
            transaction
                .pending_properties
                .get(&PID_TAG_NORMALIZED_SUBJECT_W),
            Some(&MapiValue::String("Saved subject".to_string()))
        );
    }

    #[test]
    fn saved_calendar_stream_reads_persisted_values_and_transaction_overlay() {
        let (mut session, snapshot) = saved_event_session_and_snapshot();
        let account_id = session.account_id;

        let (stream, target) =
            property_stream_data(&mut session, 1, 0x9100_0102, 1, &[], account_id, &snapshot)
                .expect("persisted named binary stream");
        assert_eq!(stream, b"persisted binary");
        assert_eq!(
            target,
            Some(StreamWriteTarget::EventProperty {
                handle: 1,
                property_tag: 0x9100_0102,
            })
        );
        sync_stream_target(&mut session, target.unwrap(), b"pending binary".to_vec())
            .expect("stage Event stream write");
        assert_eq!(
            property_stream_data(&mut session, 1, 0x9100_0102, 0, &[], account_id, &snapshot,),
            Some((b"pending binary".to_vec(), None))
        );

        let (unicode_as_string8, _) =
            property_stream_data(&mut session, 1, 0x9101_001E, 0, &[], account_id, &snapshot)
                .expect("persisted named Unicode stream through String8 alias");
        assert_eq!(unicode_as_string8, b"persisted unicode\0");

        let MapiObject::Event { transaction, .. } = session.handles.get_mut(&1).unwrap() else {
            panic!("saved Calendar event");
        };
        transaction.pending_properties.remove(&0x9100_0102);
        transaction.deleted_properties.insert(0x9100_0102);
        assert_eq!(
            property_stream_data(&mut session, 1, 0x9100_0102, 0, &[], account_id, &snapshot,),
            Some((Vec::new(), None))
        );
    }

    #[test]
    fn saved_calendar_stream_enforces_write_state_and_keeps_html_aliases_coherent() {
        let (mut session, snapshot) = saved_event_session_and_snapshot();
        let account_id = session.account_id;

        for property_tag in [
            PID_TAG_ENTRY_ID,
            (PID_LID_LOCATION_W_TAG & 0xFFFF_0000) | 0x0102,
        ] {
            assert!(property_stream_data(
                &mut session,
                1,
                property_tag,
                1,
                &[],
                account_id,
                &snapshot,
            )
            .is_none());
        }
        let MapiObject::Event { transaction, .. } = session.handles.get_mut(&1).unwrap() else {
            panic!("saved Calendar event");
        };
        transaction.import_disposition = MapiEventImportDisposition::IgnoreOlderOrSame;
        assert!(
            property_stream_data(&mut session, 1, 0x9100_0102, 1, &[], account_id, &snapshot,)
                .is_none()
        );
        let MapiObject::Event { transaction, .. } = session.handles.get_mut(&1).unwrap() else {
            panic!("saved Calendar event");
        };
        transaction.import_disposition = MapiEventImportDisposition::Apply;

        let (_, target) =
            message_body_stream_data(&session, 1, PID_TAG_HTML_BINARY, 2, &[], &[], &snapshot)
                .expect("writable saved Event HTML stream");
        sync_stream_target(
            &mut session,
            target.expect("saved Event HTML write target"),
            b"<b>saved</b>".to_vec(),
        )
        .expect("stage saved Event HTML stream");
        let MapiObject::Event { transaction, .. } = session.handles.get(&1).unwrap() else {
            panic!("saved Calendar event");
        };
        assert_eq!(
            transaction.pending_properties.get(&PID_TAG_BODY_HTML_W),
            Some(&MapiValue::String("<b>saved</b>".to_string()))
        );
        assert_eq!(
            transaction.pending_properties.get(&PID_TAG_HTML_BINARY),
            Some(&MapiValue::Binary(b"<b>saved</b>".to_vec()))
        );
    }
}
