use super::properties::*;
use super::session::*;
use super::sync::*;
use super::tables::*;
use super::wire::{MapiError, MapiPropertyType, MapiRestrictionType, RopId};
use super::*;

mod attachments;
mod buffer;
mod debug;
mod errors;
mod event_properties;
mod logon;
mod named_properties;
mod object_ids;
mod parse;
mod property_limits;
mod property_rows;
mod receive_folders;
mod recipients;
mod request_reader;
mod responses;
mod restrictions;
mod serialize;
mod typed_requests;

pub(in crate::mapi) use attachments::*;
pub(in crate::mapi) use buffer::*;
pub(in crate::mapi) use debug::*;
pub(in crate::mapi) use errors::*;
pub(in crate::mapi) use event_properties::*;
pub(in crate::mapi) use logon::*;
pub(in crate::mapi) use named_properties::*;
pub(in crate::mapi) use object_ids::*;
pub(in crate::mapi) use parse::*;
use property_limits::*;
pub(in crate::mapi) use receive_folders::*;
#[cfg(test)]
pub(in crate::mapi) use recipients::*;
pub(in crate::mapi) use request_reader::*;
pub(in crate::mapi) use responses::*;
pub(in crate::mapi) use restrictions::*;
pub(in crate::mapi) use serialize::*;
pub(in crate::mapi) use typed_requests::*;

#[allow(dead_code)]
pub(in crate::mapi) fn rop_get_properties_specific_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    rop_get_properties_specific_response_with_custom(
        request,
        object,
        principal,
        mailboxes,
        emails,
        snapshot,
        &HashMap::new(),
        usize::MAX,
    )
}

pub(in crate::mapi) fn rop_get_properties_specific_response_with_custom(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    custom_values: &HashMap<u32, Vec<u8>>,
    response_size_limit: usize,
) -> Vec<u8> {
    let mut response = vec![0x07, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let columns = request.property_tags();
    let mut unsupported_tags = unsupported_specific_property_tags(
        object, principal, mailboxes, emails, snapshot, &columns,
    );
    unsupported_tags.retain(|tag| !custom_values.contains_key(tag));
    let size_limited_properties = size_limited_specific_properties(
        request,
        object,
        principal,
        mailboxes,
        emails,
        snapshot,
        &columns,
        &unsupported_tags,
        custom_values,
        response_size_limit,
    );
    let row = match object {
        Some(MapiObject::Logon) => {
            log_get_properties_specific_debug(
                request, object, principal, &columns, mailboxes, emails, snapshot,
            );
            write_logon_property_row(&mut response, principal, &columns);
            return response;
        }
        Some(MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        }) => {
            let Some(_email) = message_for_id(*folder_id, *message_id, mailboxes, emails)
                .or_else(|| {
                    search_folder_message_for_id(snapshot, *folder_id, *message_id)
                        .map(|message| &message.email)
                })
                .or(saved_email.as_ref().map(|saved| &saved.email))
            else {
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x07",
                    object_kind = "message",
                    folder_id = %format!("{folder_id:#018x}"),
                    message_id = %format!("{message_id:#018x}"),
                    requested_property_tag_count = columns.len(),
                    requested_property_tags = %columns
                        .iter()
                        .map(|tag| format!("{tag:#010x}"))
                        .collect::<Vec<_>>()
                        .join(","),
                    base_email_count = emails.len(),
                    failure_reason = "message_identity_not_visible_for_getprops",
                    "rca debug mapi get properties specific"
                );
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::PendingMessage {
            properties,
            recipients,
            ..
        }) => serialize_pending_message_row(principal, properties, recipients, &columns),
        Some(MapiObject::PendingAssociatedMessage { properties, .. }) => {
            serialize_pending_associated_message_row(principal, properties, &columns)
        }
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
        }) => {
            let Some(_contact) = snapshot.contact_for_id(*folder_id, *contact_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::PendingContact { properties, .. }) => {
            serialize_pending_contact_row(principal, properties, &columns)
        }
        Some(MapiObject::Event {
            folder_id,
            event_id,
            ..
        }) => {
            let Some(_event) = snapshot.event_for_id(*folder_id, *event_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::PendingEvent { properties, .. }) => {
            serialize_pending_event_row(principal, properties, &columns)
        }
        Some(MapiObject::PendingTask { properties, .. }) => {
            serialize_pending_task_row(principal, properties, &columns)
        }
        Some(MapiObject::PendingNote { properties, .. }) => {
            serialize_pending_note_row(principal, properties, &columns)
        }
        Some(MapiObject::PendingJournalEntry { properties, .. }) => {
            serialize_pending_journal_entry_row(principal, properties, &columns)
        }
        Some(MapiObject::PendingConversationAction { properties, .. }) => {
            serialize_pending_conversation_action_row(properties, &columns)
        }
        Some(MapiObject::PendingNavigationShortcut { properties, .. }) => {
            serialize_pending_navigation_shortcut_row(properties, principal, &columns)
        }
        Some(MapiObject::Task { folder_id, task_id }) => {
            let Some(_task) = snapshot.task_for_id(*folder_id, *task_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::Note { folder_id, note_id }) => {
            let Some(_note) = snapshot.note_for_id(*folder_id, *note_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        }) => {
            let Some(_entry) = snapshot.journal_entry_for_id(*folder_id, *journal_entry_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
            pending_properties,
            deleted_properties,
        }) => {
            let Some(message) = snapshot
                .navigation_shortcut_table_message_for_id(*shortcut_id)
                .filter(|message| message.folder_id == *folder_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            let message = navigation_shortcut_with_pending_properties(
                &message,
                principal.account_id,
                pending_properties,
                deleted_properties,
            );
            serialize_navigation_shortcut_row(&message, Some(principal), &columns)
        }
        Some(MapiObject::CommonViewNamedView { folder_id, view_id }) => {
            let Some(message) = snapshot.named_view_message_for_folder_and_id(*folder_id, *view_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_common_view_named_view_row_with_mailbox_guid(
                &message,
                principal.account_id,
                &columns,
            )
        }
        Some(MapiObject::SearchFolderDefinitionMessage {
            folder_id,
            message_id,
        }) => {
            let Some(definition) = (folder_id == &COMMON_VIEWS_FOLDER_ID)
                .then(|| {
                    snapshot.common_views_table_messages().find_map(|message| {
                        if let crate::mapi_store::MapiCommonViewsMessage::SearchFolderDefinition(
                            definition,
                        ) = message
                        {
                            (crate::mapi::identity::mapped_mapi_object_id(&definition.id)
                                == Some(*message_id))
                            .then_some(definition)
                        } else {
                            None
                        }
                    })
                })
                .flatten()
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_search_folder_definition_row_with_mailbox_guid(
                &definition,
                principal.account_id,
                &columns,
            )
        }
        Some(MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
            ..
        }) => {
            let Some(message) = snapshot
                .associated_config_message_for_id(*config_id)
                .or_else(|| saved_message.clone())
                .filter(|message| message.folder_id == *folder_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_associated_config_row_with_mailbox_guid(
                &message,
                principal.account_id,
                &columns,
            )
        }
        Some(MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
            ..
        }) => {
            let Some(message) = snapshot
                .conversation_action_table_message_for_id(*conversation_action_id)
                .filter(|message| message.folder_id == *folder_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_conversation_action_row(&message, &columns)
        }
        Some(MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
        }) => {
            let Some(message) = snapshot
                .delegate_freebusy_message_for_id(*message_id)
                .filter(|message| message.folder_id == *folder_id)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_delegate_freebusy_row(message, &columns)
        }
        Some(MapiObject::RecoverableItem { folder_id, item_id }) => {
            let Some(item) = snapshot.recoverable_item_for_id(*folder_id, *item_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_recoverable_item_row(item, &columns)
        }
        Some(MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        }) => {
            if snapshot
                .public_folder_item_for_id(*folder_id, *item_id)
                .is_none()
            {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::Folder {
            folder_id,
            properties,
        }) => serialize_session_folder_row(
            *folder_id, properties, principal, mailboxes, snapshot, &columns,
        ),
        Some(MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        }) => {
            if snapshot
                .attachment_for_message(*folder_id, *message_id, *attach_num)
                .is_none()
            {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            }
            serialize_object_property_row_with_custom(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                &columns,
                custom_values,
            )
        }
        Some(MapiObject::PendingAttachment {
            attach_num,
            properties,
            data,
            ..
        }) => serialize_pending_attachment_row(*attach_num, properties, data, &columns),
        Some(MapiObject::SavedAttachment {
            attach_num,
            file_reference,
            file_name,
            media_type,
            disposition,
            content_id,
            size_octets,
            ..
        }) => serialize_saved_attachment_row(
            *attach_num,
            file_reference,
            file_name,
            media_type,
            disposition.as_deref(),
            content_id.as_deref(),
            *size_octets,
            &columns,
        ),
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| {
                    serialize_folder_row_with_context_and_version(
                        mailbox,
                        mailboxes,
                        &columns,
                        principal.account_id,
                        snapshot.folder_version(folder_id),
                    )
                })
                .or_else(|| {
                    snapshot
                        .collaboration_folder_for_id(folder_id)
                        .map(|folder| {
                            serialize_collaboration_folder_row_with_context_and_version(
                                folder,
                                &columns,
                                associated_folder_message_count(folder_id, snapshot),
                                snapshot.folder_version(folder_id),
                            )
                        })
                })
                .unwrap_or_else(|| {
                    serialize_special_folder_row_with_version(
                        folder_id,
                        mailboxes,
                        &columns,
                        Some(principal),
                        snapshot.folder_version(folder_id),
                    )
                })
        }
    };
    log_get_properties_specific_debug(
        request, object, principal, &columns, mailboxes, emails, snapshot,
    );
    if unsupported_tags.is_empty()
        && !size_limited_properties.iter().any(|value| *value)
        && !columns
            .iter()
            .any(|tag| get_properties_specific_typed_value_tag(object, *tag).is_some())
    {
        write_standard_property_row(&mut response, &row);
    } else {
        write_flagged_property_row(
            &mut response,
            object,
            principal,
            mailboxes,
            emails,
            snapshot,
            &columns,
            &unsupported_tags,
            &size_limited_properties,
            custom_values,
        );
    }
    response
}

fn serialize_object_property_row_with_custom(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    custom_values: &HashMap<u32, Vec<u8>>,
) -> Vec<u8> {
    let mut row = Vec::new();
    for tag in columns {
        if let Some(value) = custom_values.get(tag) {
            row.extend_from_slice(value);
        } else {
            row.extend_from_slice(&serialize_object_property(
                object,
                principal,
                mailboxes,
                emails,
                snapshot,
                get_properties_specific_value_tag(object, *tag),
            ));
        }
    }
    row
}

fn unsupported_specific_property_tags(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
) -> Vec<u32> {
    columns
        .iter()
        .copied()
        .filter(|tag| {
            property_is_unsupported_for_object(object, principal, *tag)
                || fallback_default_specific_property(
                    object, principal, mailboxes, emails, snapshot, *tag,
                )
        })
        .collect()
}

fn fallback_default_specific_property(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> bool {
    if event_object_property_is_deleted(object, tag) {
        return true;
    }
    if navigation_shortcut_object_property_is_deleted(object, tag) {
        return true;
    }
    if let Some(MapiObject::PendingAssociatedMessage { properties, .. }) = object {
        let storage_tag = canonical_property_storage_tag(tag);
        // [MS-OXCMSG] 3.2.5.2 and [MS-OXCDATA] 2.8.1.2: an unwritten
        // client-owned configuration stream is absent, not an empty value.
        if storage_tag == PID_TAG_ROAMING_DICTIONARY && !properties.contains_key(&storage_tag) {
            return true;
        }
    }
    if let Some(MapiObject::Message {
        folder_id,
        message_id,
        saved_email,
        ..
    }) = object
    {
        let value_tag = get_properties_specific_value_tag(object, tag);
        return message_for_id(*folder_id, *message_id, mailboxes, emails)
            .or_else(|| {
                search_folder_message_for_id(snapshot, *folder_id, *message_id)
                    .map(|message| &message.email)
            })
            .or(saved_email.as_ref().map(|saved| &saved.email))
            .is_some_and(|email| email_property_value(email, value_tag).is_none());
    }
    if !matches!(
        object,
        Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
            | Some(MapiObject::Folder { .. })
            | Some(MapiObject::AssociatedConfig { .. } | MapiObject::CommonViewNamedView { .. })
            | None
    ) {
        return false;
    }
    let tag = get_properties_specific_value_tag(object, tag);
    let encoded = serialize_object_property(object, principal, mailboxes, emails, snapshot, tag);
    let mut default_value = Vec::new();
    write_property_default(&mut default_value, tag);
    if encoded == default_value
        && canonical_property_storage_tag(tag) == PID_TAG_DEFAULT_VIEW_ENTRY_ID
    {
        return true;
    }
    encoded == default_value
        && !modeled_zero_or_default_property(object, tag)
        && !associated_config_modeled_property(object, principal, snapshot, tag)
}

fn associated_config_modeled_property(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> bool {
    let Some(MapiObject::AssociatedConfig {
        folder_id,
        config_id,
        saved_message,
    }) = object
    else {
        return false;
    };
    snapshot
        .associated_config_message_for_id(*config_id)
        .or_else(|| saved_message.clone())
        .filter(|message| message.folder_id == *folder_id)
        .is_some_and(|message| {
            associated_config_property_value_with_mailbox_guid(&message, principal.account_id, tag)
                .is_some()
        })
}

fn flagged_property_error_code(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> u32 {
    if property_is_unsupported_for_object(object, principal, tag) {
        0x8004_0102
    } else if fallback_default_specific_property(
        object, principal, mailboxes, emails, snapshot, tag,
    ) {
        ROP_ERROR_NOT_FOUND
    } else if property_tag_debug_name(tag) == "unknown" {
        0x8004_0102
    } else {
        0x8004_0102
    }
}

fn write_flagged_property_row(
    response: &mut Vec<u8>,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    unsupported_tags: &[u32],
    size_limited_properties: &[bool],
    custom_values: &HashMap<u32, Vec<u8>>,
) {
    response.push(1);
    for (index, tag) in columns.iter().enumerate() {
        if size_limited_properties.get(index) == Some(&true) {
            if let Some((_value_tag, property_type)) =
                get_properties_specific_typed_value_tag(object, *tag)
            {
                write_u16(response, property_type);
            }
            write_flagged_property_error(response, 0x8007_000E);
        } else if unsupported_tags.contains(tag) {
            write_flagged_property_error(
                response,
                flagged_property_error_code(object, principal, mailboxes, emails, snapshot, *tag),
            );
        } else if let Some(value) = custom_values.get(tag) {
            response.push(0);
            response.extend_from_slice(value);
        } else if let Some((value_tag, property_type)) =
            get_properties_specific_typed_value_tag(object, *tag)
        {
            write_u16(response, property_type);
            response.push(0);
            response.extend_from_slice(&serialize_object_property(
                object, principal, mailboxes, emails, snapshot, value_tag,
            ));
        } else {
            response.push(0);
            response.extend_from_slice(&serialize_object_property(
                object, principal, mailboxes, emails, snapshot, *tag,
            ));
        }
    }
}

fn write_flagged_property_error(response: &mut Vec<u8>, error_code: u32) {
    response.push(0x0A);
    write_u32(response, error_code);
}

fn utf16le_bytes(value: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(value.encode_utf16().count() * 2);
    for unit in value.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

pub(in crate::mapi) fn property_is_unsupported_for_object(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    tag: u32,
) -> bool {
    let value_tag = get_properties_specific_value_tag(object, tag);
    if MapiPropertyTag::new(value_tag).property_type().is_none() {
        return true;
    }
    if matches!(
        object,
        Some(MapiObject::Note { .. } | MapiObject::PendingNote { .. })
    ) && matches!(
        canonical_property_storage_tag(value_tag),
        PID_LID_NOTE_HEIGHT_TAG | PID_LID_NOTE_WIDTH_TAG | PID_LID_NOTE_X_TAG | PID_LID_NOTE_Y_TAG
    ) {
        return true;
    }
    matches!(object, Some(MapiObject::Logon))
        && logon_property_value(principal, value_tag).is_none()
}

fn get_properties_specific_value_tag(object: Option<&MapiObject>, tag: u32) -> u32 {
    get_properties_specific_typed_value_tag(object, tag)
        .map(|(value_tag, _property_type)| value_tag)
        .unwrap_or(tag)
}

fn get_properties_specific_typed_value_tag(
    object: Option<&MapiObject>,
    tag: u32,
) -> Option<(u32, u16)> {
    let requested_type = MapiPropertyTag::new(tag).property_type_code();
    if requested_type != 0x0000 && requested_type != 0x0001 {
        return None;
    }
    let property_id = tag & 0xFFFF_0000;
    let value_tag = match property_id {
        0x001A_0000 => PID_TAG_MESSAGE_CLASS_W,
        0x0037_0000 => PID_TAG_SUBJECT_W,
        0x0E07_0000 => PID_TAG_MESSAGE_FLAGS,
        0x0E1D_0000 => PID_TAG_NORMALIZED_SUBJECT_W,
        0x1000_0000 => PID_TAG_BODY_W,
        0x1013_0000 => PID_TAG_BODY_HTML_W,
        0x3001_0000 => PID_TAG_DISPLAY_NAME_W,
        _ => get_properties_specific_candidate_tags(object)
            .into_iter()
            .find(|candidate| (*candidate & 0xFFFF_0000) == property_id)?,
    };
    Some((value_tag, (value_tag & 0xFFFF) as u16))
}

fn get_properties_specific_candidate_tags(object: Option<&MapiObject>) -> Vec<u32> {
    match object {
        Some(MapiObject::Logon) => default_store_property_tags(),
        Some(MapiObject::PublicFolderLogon) => vec![PID_TAG_PRIVATE],
        Some(MapiObject::Contact { .. } | MapiObject::PendingContact { .. }) => {
            default_contact_property_tags()
        }
        Some(MapiObject::Event { .. } | MapiObject::PendingEvent { .. }) => {
            default_event_property_tags()
        }
        Some(MapiObject::Task { .. } | MapiObject::PendingTask { .. }) => {
            default_task_property_tags()
        }
        Some(MapiObject::Note { .. } | MapiObject::PendingNote { .. }) => {
            default_note_property_tags()
        }
        Some(MapiObject::JournalEntry { .. } | MapiObject::PendingJournalEntry { .. }) => {
            default_journal_entry_property_tags()
        }
        Some(MapiObject::Attachment { .. })
        | Some(MapiObject::PendingAttachment { .. })
        | Some(MapiObject::SavedAttachment { .. }) => default_attachment_columns(),
        Some(
            MapiObject::Message { .. }
            | MapiObject::AssociatedConfig { .. }
            | MapiObject::PublicFolderItem { .. }
            | MapiObject::PendingAssociatedMessage { .. }
            | MapiObject::PendingMessage { .. },
        ) => default_message_property_tags(),
        Some(
            MapiObject::ConversationAction { .. } | MapiObject::PendingConversationAction { .. },
        ) => default_conversation_action_property_tags(),
        _ => default_folder_property_tags(),
    }
}

fn modeled_zero_or_default_property(object: Option<&MapiObject>, tag: u32) -> bool {
    let storage_tag = canonical_property_storage_tag(tag);
    match object {
        Some(MapiObject::Logon) => matches!(
            tag,
            PID_TAG_PRIVATE
                | PID_TAG_OUTLOOK_STORE_STATE
                | PID_TAG_RESOURCE_FLAGS
                | PID_TAG_PST_PATH_W
                | PID_TAG_SERVER_CONNECTED_ICON
                | PID_TAG_SERVER_ACCOUNT_ICON
        ),
        Some(MapiObject::PublicFolderLogon) => matches!(tag, PID_TAG_PRIVATE),
        Some(MapiObject::AssociatedConfig { .. }) => {
            matches!(
                storage_tag,
                PID_TAG_INSTANCE_NUM
                    | PID_TAG_MESSAGE_FLAGS
                    | PID_TAG_MESSAGE_STATUS
                    | PID_TAG_ACCESS_LEVEL
                    | PID_TAG_SENT_MAIL_SVR_EID
                    | PID_TAG_ASSOCIATED
                    | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
            ) || associated_config_contacts_helper_default_property(object, tag)
        }
        Some(MapiObject::CommonViewNamedView { .. }) => matches!(
            storage_tag,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
                | PID_TAG_VIEW_DESCRIPTOR_BINARY
        ),
        Some(MapiObject::DelegateFreeBusyMessage { .. }) => matches!(
            storage_tag,
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE
                | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
                | 0x6842_000B
                | 0x6843_000B
                | 0x6844_101F
                | 0x6845_1102
                | 0x684A_101F
                | 0x684B_000B
                | 0x686B_1003
                | 0x686D_000B
                | 0x686E_000B
                | 0x686F_000B
                | 0x6870_1102
                | 0x6871_1003
                | 0x6872_001F
        ),
        Some(MapiObject::Folder { folder_id, .. }) => {
            is_modeled_empty_special_folder_class_property(*folder_id, storage_tag)
                || matches!(
                    storage_tag,
                    PID_TAG_CONTENT_COUNT
                        | PID_TAG_CONTENT_UNREAD_COUNT
                        | PID_TAG_DELETED_COUNT_TOTAL
                        | PID_TAG_SUBFOLDERS
                        | PID_TAG_PARENT_FOLDER_ID
                        | PID_TAG_PARENT_SOURCE_KEY
                        | PID_TAG_FOLDER_TYPE
                        | PID_TAG_RETENTION_PERIOD
                        | PID_TAG_RETENTION_FLAGS
                        | PID_TAG_ARCHIVE_PERIOD
                        | PID_TAG_FOLDER_FORM_FLAGS
                        | PID_TAG_FOLDER_VIEWS_ONLY
                        | PID_TAG_DEFAULT_FORM_NAME_W
                        | PID_TAG_FOLDER_FORM_STORAGE
                        | PID_TAG_FOLDER_VIEWLIST_FLAGS
                        | OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C
                )
        }
        None => {
            matches!(
                storage_tag,
                PID_TAG_CONTENT_COUNT
                    | PID_TAG_CONTENT_UNREAD_COUNT
                    | PID_TAG_DELETED_COUNT_TOTAL
                    | PID_TAG_SUBFOLDERS
                    | PID_TAG_PARENT_FOLDER_ID
                    | PID_TAG_PARENT_SOURCE_KEY
                    | PID_TAG_FOLDER_TYPE
                    | PID_TAG_RETENTION_PERIOD
                    | PID_TAG_RETENTION_FLAGS
                    | PID_TAG_ARCHIVE_PERIOD
                    | PID_TAG_FOLDER_FORM_FLAGS
                    | PID_TAG_FOLDER_VIEWS_ONLY
                    | PID_TAG_DEFAULT_FORM_NAME_W
                    | PID_TAG_FOLDER_FORM_STORAGE
                    | PID_TAG_FOLDER_VIEWLIST_FLAGS
            )
        }
        _ => false,
    }
}

fn associated_config_contacts_helper_default_property(
    object: Option<&MapiObject>,
    tag: u32,
) -> bool {
    let Some(MapiObject::AssociatedConfig {
        saved_message: Some(message),
        ..
    }) = object
    else {
        return false;
    };
    if !matches!(
        message.message_class.as_str(),
        "IPM.Microsoft.ContactLink.TimeStamp" | "IPM.Microsoft.OSC.ContactSync"
    ) {
        return false;
    }
    let property_id = MapiPropertyTag::new(tag).property_id();
    let property_id = u32::from(property_id);
    property_id == u32::from(MapiPropertyTag::new(PID_NAME_OSC_CONTACT_SOURCES_TAG).property_id())
        || matches!(
            property_id,
            PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1
                | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA
                | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC
                | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED
        )
}

fn is_modeled_empty_special_folder_class_property(folder_id: u64, storage_tag: u32) -> bool {
    matches!(
        folder_id,
        ROOT_FOLDER_ID
            | DEFERRED_ACTION_FOLDER_ID
            | SPOOLER_QUEUE_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | VIEWS_FOLDER_ID
    ) && matches!(
        storage_tag,
        PID_TAG_CONTAINER_CLASS_W
            | PID_TAG_MESSAGE_CLASS_W
            | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8
            | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W
    )
}

pub(in crate::mapi) fn rop_get_properties_all_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x08, request.input_handle_index().unwrap_or(0), 0x8004_0102);
    };
    if let MapiObject::Event {
        folder_id,
        event_id,
        ..
    } = object
    {
        if snapshot.event_for_id(*folder_id, *event_id).is_none() {
            return rop_error_response(
                0x08,
                request.input_handle_index().unwrap_or(0),
                0x8004_010F,
            );
        }
    }
    let mut response = vec![0x08, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let size_limit = request_property_size_limit(request);
    let want_unicode = request_get_properties_all_want_unicode(request);
    let tags = match object {
        MapiObject::Logon => default_store_property_tags(),
        MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID | INBOX_FOLDER_ID,
            ..
        } => default_folder_property_tags_with_identity(),
        MapiObject::Attachment { .. }
        | MapiObject::PendingAttachment { .. }
        | MapiObject::SavedAttachment { .. } => default_attachment_columns(),
        MapiObject::Message { .. }
        | MapiObject::PublicFolderItem { .. }
        | MapiObject::PendingMessage { .. } => default_message_property_tags(),
        MapiObject::Contact { .. } | MapiObject::PendingContact { .. } => {
            default_contact_property_tags()
        }
        MapiObject::Event { .. } | MapiObject::PendingEvent { .. } => default_event_property_tags(),
        MapiObject::Task { .. } | MapiObject::PendingTask { .. } => default_task_property_tags(),
        MapiObject::Note { .. } | MapiObject::PendingNote { .. } => default_note_property_tags(),
        MapiObject::JournalEntry { .. } | MapiObject::PendingJournalEntry { .. } => {
            default_journal_entry_property_tags()
        }
        MapiObject::ConversationAction { .. } | MapiObject::PendingConversationAction { .. } => {
            default_conversation_action_property_tags()
        }
        _ => default_folder_property_tags(),
    };
    response.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        let tag = get_properties_all_response_tag(tag, want_unicode);
        let value =
            serialize_object_property(Some(object), principal, mailboxes, emails, snapshot, tag);
        if size_limit != 0 && value.len() > size_limit {
            write_u32(&mut response, property_error_tag(tag));
            write_u32(&mut response, 0x8007_000E);
        } else {
            write_u32(&mut response, tag);
            response.extend_from_slice(&value);
        }
    }
    response
}

fn request_get_properties_all_want_unicode(request: &RopRequest) -> bool {
    request
        .payload
        .get(2..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .unwrap_or(1)
        != 0
}

fn get_properties_all_response_tag(property_tag: u32, want_unicode: bool) -> u32 {
    if want_unicode {
        return property_tag;
    }
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::String) => (property_tag & 0xFFFF_0000) | 0x001E,
        Some(MapiPropertyType::MultipleString) => (property_tag & 0xFFFF_0000) | 0x101E,
        _ => property_tag,
    }
}

fn property_error_tag(property_tag: u32) -> u32 {
    (property_tag & 0xFFFF_0000) | 0x000A
}

fn default_folder_property_tags_with_identity() -> Vec<u32> {
    let mut tags = default_folder_property_tags();
    tags.extend(default_folder_identity_property_tags());
    tags
}

pub(in crate::mapi) fn serialize_object_property(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> Vec<u8> {
    match object {
        Some(MapiObject::Logon) => serialize_logon_row(principal, &[tag]),
        Some(MapiObject::PublicFolderLogon) if tag == PID_TAG_PRIVATE => {
            let mut row = Vec::new();
            write_mapi_value(&mut row, tag, &MapiValue::Bool(false));
            row
        }
        Some(MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        }) => message_for_id(*folder_id, *message_id, mailboxes, emails)
            .or_else(|| {
                search_folder_message_for_id(snapshot, *folder_id, *message_id)
                    .map(|message| &message.email)
            })
            .or(saved_email.as_ref().map(|saved| &saved.email))
            .map(|email| serialize_message_row(email, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingMessage {
            properties,
            recipients,
            ..
        }) => serialize_pending_message_row(principal, properties, recipients, &[tag]),
        Some(MapiObject::PendingAssociatedMessage { properties, .. }) => {
            serialize_pending_associated_message_row(principal, properties, &[tag])
        }
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
        }) => snapshot
            .contact_for_id(*folder_id, *contact_id)
            .map(|contact| {
                serialize_contact_row(&contact.contact, contact.id, contact.folder_id, &[tag])
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingContact { properties, .. }) => {
            serialize_pending_contact_row(principal, properties, &[tag])
        }
        Some(object @ MapiObject::Event { .. }) => {
            serialize_event_object_property(object, snapshot, tag)
        }
        Some(MapiObject::PendingEvent { properties, .. }) => {
            serialize_pending_event_row(principal, properties, &[tag])
        }
        Some(MapiObject::PendingTask { properties, .. }) => {
            serialize_pending_task_row(principal, properties, &[tag])
        }
        Some(MapiObject::PendingNote { properties, .. }) => {
            serialize_pending_note_row(principal, properties, &[tag])
        }
        Some(MapiObject::PendingJournalEntry { properties, .. }) => {
            serialize_pending_journal_entry_row(principal, properties, &[tag])
        }
        Some(MapiObject::PendingConversationAction { properties, .. }) => {
            serialize_pending_conversation_action_row(properties, &[tag])
        }
        Some(MapiObject::PendingNavigationShortcut { properties, .. }) => {
            serialize_pending_navigation_shortcut_row(properties, principal, &[tag])
        }
        Some(MapiObject::Task { folder_id, task_id }) => snapshot
            .task_for_id(*folder_id, *task_id)
            .map(|task| serialize_task_row(&task.task, task.id, task.folder_id, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::Note { folder_id, note_id }) => snapshot
            .note_for_id(*folder_id, *note_id)
            .map(|note| serialize_note_row(&note.note, note.id, note.folder_id, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        }) => snapshot
            .journal_entry_for_id(*folder_id, *journal_entry_id)
            .map(|entry| {
                serialize_journal_entry_row(&entry.entry, entry.id, entry.folder_id, &[tag])
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
            pending_properties,
            deleted_properties,
        }) => snapshot
            .navigation_shortcut_table_message_for_id(*shortcut_id)
            .filter(|message| message.folder_id == *folder_id)
            .map(|message| {
                let message = navigation_shortcut_with_pending_properties(
                    &message,
                    principal.account_id,
                    pending_properties,
                    deleted_properties,
                );
                serialize_navigation_shortcut_row(&message, Some(principal), &[tag])
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::CommonViewNamedView { folder_id, view_id }) => snapshot
            .named_view_message_for_folder_and_id(*folder_id, *view_id)
            .map(|message| {
                serialize_common_view_named_view_row_with_mailbox_guid(
                    &message,
                    principal.account_id,
                    &[tag],
                )
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
            ..
        }) => snapshot
            .associated_config_message_for_id(*config_id)
            .or_else(|| saved_message.clone())
            .filter(|message| message.folder_id == *folder_id)
            .map(|message| {
                serialize_associated_config_row_with_mailbox_guid(
                    &message,
                    principal.account_id,
                    &[tag],
                )
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
            ..
        }) => snapshot
            .conversation_action_table_message_for_id(*conversation_action_id)
            .filter(|message| message.folder_id == *folder_id)
            .map(|message| serialize_conversation_action_row(&message, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
        }) => snapshot
            .delegate_freebusy_message_for_id(*message_id)
            .filter(|message| message.folder_id == *folder_id)
            .map(|message| serialize_delegate_freebusy_row(message, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::RecoverableItem { folder_id, item_id }) => snapshot
            .recoverable_item_for_id(*folder_id, *item_id)
            .map(|item| serialize_recoverable_item_row(item, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        }) => snapshot
            .public_folder_item_for_id(*folder_id, *item_id)
            .map(|item| serialize_public_folder_item_row(item, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::Folder {
            folder_id,
            properties,
        }) => serialize_session_folder_row(
            *folder_id,
            properties,
            principal,
            mailboxes,
            snapshot,
            &[tag],
        ),
        Some(MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        }) => snapshot
            .attachment_for_message(*folder_id, *message_id, *attach_num)
            .map(|attachment| serialize_attachment_row(attachment, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingAttachment {
            attach_num,
            properties,
            data,
            ..
        }) => serialize_pending_attachment_row(*attach_num, properties, data, &[tag]),
        Some(MapiObject::SavedAttachment {
            attach_num,
            file_reference,
            file_name,
            media_type,
            disposition,
            content_id,
            size_octets,
            ..
        }) => serialize_saved_attachment_row(
            *attach_num,
            file_reference,
            file_name,
            media_type,
            disposition.as_deref(),
            content_id.as_deref(),
            *size_octets,
            &[tag],
        ),
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| {
                    serialize_folder_row_with_context_and_version(
                        mailbox,
                        mailboxes,
                        &[tag],
                        principal.account_id,
                        snapshot.folder_version(folder_id),
                    )
                })
                .or_else(|| {
                    snapshot
                        .collaboration_folder_for_id(folder_id)
                        .map(|folder| {
                            serialize_collaboration_folder_row_with_context_and_version(
                                folder,
                                &[tag],
                                associated_folder_message_count(folder_id, snapshot),
                                snapshot.folder_version(folder_id),
                            )
                        })
                })
                .unwrap_or_else(|| {
                    serialize_special_folder_row_with_version(
                        folder_id,
                        mailboxes,
                        &[tag],
                        Some(principal),
                        snapshot.folder_version(folder_id),
                    )
                })
        }
    }
}

fn serialize_session_folder_row(
    folder_id: u64,
    properties: &HashMap<u32, MapiValue>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        let storage_tag = canonical_property_storage_tag(*column);
        if storage_tag == PID_TAG_FOLDER_TYPE
            && snapshot
                .search_folder_definition_for_folder_id(folder_id)
                .is_some()
        {
            write_mapi_value(&mut row, *column, &MapiValue::U32(FOLDER_SEARCH));
            continue;
        }

        if let Some(value) = snapshot
            .folder_version(folder_id)
            .and_then(|version| folder_version_property_value(version, *column))
        {
            write_mapi_value(&mut row, *column, &value);
            continue;
        }

        if let Some(value) = properties
            .get(&storage_tag)
            .or_else(|| properties.get(column))
        {
            write_mapi_value(&mut row, *column, value);
            continue;
        }

        if (is_advertised_special_folder(folder_id)
            || snapshot
                .search_folder_definition_for_folder_id(folder_id)
                .is_some())
            && storage_tag == PID_TAG_CONTENT_COUNT
        {
            write_mapi_value(
                &mut row,
                *column,
                &MapiValue::U32(folder_message_count(folder_id, mailboxes, &[], snapshot)),
            );
            continue;
        }

        if let Some(definition) = snapshot.search_folder_definition_for_folder_id(folder_id) {
            if let Some(value) = search_folder_definition_property_value(
                definition,
                folder_id,
                *column,
                principal.account_id,
            ) {
                write_mapi_value(&mut row, *column, &value);
                continue;
            }
        }

        if folder_id == IPM_SUBTREE_FOLDER_ID && storage_tag == PID_TAG_OST_OSTID {
            write_mapi_value(
                &mut row,
                *column,
                &MapiValue::Binary(ipm_subtree_ost_ostid(principal)),
            );
            continue;
        }

        if matches!(folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
            if let Some(value) =
                special_folder_identification_property_value(principal.account_id, *column)
            {
                write_mapi_value(&mut row, *column, &value);
                continue;
            }
        }

        let value = folder_row_for_id(folder_id, mailboxes)
            .map(|mailbox| {
                serialize_folder_row_with_context_and_version(
                    mailbox,
                    mailboxes,
                    &[*column],
                    principal.account_id,
                    snapshot.folder_version(folder_id),
                )
            })
            .or_else(|| {
                snapshot
                    .collaboration_folder_for_id(folder_id)
                    .map(|folder| {
                        serialize_collaboration_folder_row_with_context_and_version(
                            folder,
                            &[*column],
                            associated_folder_message_count(folder_id, snapshot),
                            snapshot.folder_version(folder_id),
                        )
                    })
            })
            .or_else(|| {
                snapshot
                    .public_folder_for_id(folder_id)
                    .map(|folder| serialize_public_folder_row(folder, &[*column]))
            })
            .unwrap_or_else(|| {
                serialize_special_folder_row_with_version(
                    folder_id,
                    mailboxes,
                    &[*column],
                    Some(principal),
                    snapshot.folder_version(folder_id),
                )
            });
        row.extend_from_slice(&value);
    }
    row
}

pub(in crate::mapi) fn folder_row_for_id(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> Option<&JmapMailbox> {
    mailboxes.iter().find(|mailbox| {
        mapi_folder_id(mailbox) == folder_id
            || mailbox.role == role_for_folder_id(folder_id).unwrap_or_default()
    })
}

#[cfg(test)]
mod tests;
