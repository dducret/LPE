use super::rop::*;
use super::session::*;
use super::sync::*;
use super::tables::*;
use super::wire::MapiPropertyType;
use super::*;
use crate::mapi::identity::{
    RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID, RECOVERABLE_ITEMS_PURGES_FOLDER_ID,
    RECOVERABLE_ITEMS_ROOT_FOLDER_ID, RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID,
};
use crate::mapi_store::{
    MapiAssociatedConfigMessage, MapiAttachment, MapiCommonViewNamedViewMessage,
    MapiConversationActionMessage, MapiMessage, MapiNavigationShortcutMessage, MapiPublicFolder,
};
use anyhow::bail;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_domain::{civil_from_days, days_from_civil};
use lpe_storage::{
    calendar_attendee_labels, normalize_calendar_email, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, CalendarOrganizerMetadata,
    CalendarParticipantMetadata, SearchFolderDefinition,
};

mod attachments;
mod calendar;
mod contact;
mod folder;
mod message;
mod named;
mod notes;
mod recurrence;
mod reminders;
mod restrictions;
mod search_folders;
mod streams;
mod tags;
mod task;
mod values;
mod views;

pub(in crate::mapi) use attachments::*;
pub(in crate::mapi) use calendar::*;
pub(in crate::mapi) use contact::*;
pub(in crate::mapi) use folder::*;
pub(in crate::mapi) use message::*;
pub(crate) use named::*;
pub(in crate::mapi) use notes::*;
use recurrence::*;
pub(in crate::mapi) use reminders::*;
pub(in crate::mapi) use restrictions::*;
pub(in crate::mapi) use search_folders::*;
#[cfg(test)]
use streams::property_stream_data;
pub(in crate::mapi) use streams::*;
use streams::{pending_body_text_property, property_tag_type};
pub(crate) use tags::*;
pub(in crate::mapi) use task::*;
pub(super) use values::*;
pub(in crate::mapi) use views::*;

pub(in crate::mapi) const NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID: [u8; 16] = [
    0xDC, 0xA7, 0x40, 0xC8, 0xC0, 0x42, 0x10, 0x1A, 0xB4, 0xB9, 0x08, 0x00, 0x2B, 0x2F, 0xE1, 0x82,
];

pub(in crate::mapi) fn rop_read_recipients_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let input_handle_index = request.input_handle_index().unwrap_or(0);
    let start = request.row_id().unwrap_or(0) as usize;

    let mut response = vec![0x0F, input_handle_index];
    write_u32(&mut response, 0);

    let available_rows = match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
            saved_email,
            ..
        }) => {
            let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails)
                .or_else(|| {
                    search_folder_message_for_id(snapshot, *folder_id, *message_id)
                        .map(|message| &message.email)
                })
                .or(saved_email.as_ref().map(|saved| &saved.email))
            else {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            };
            message_recipients(email)
                .into_iter()
                .enumerate()
                .map(|(offset, recipient)| {
                    (
                        offset as u32,
                        recipient.recipient_type,
                        serialize_recipient_row(recipient.address),
                    )
                })
                .collect::<Vec<_>>()
        }
        Some(MapiObject::PendingMessage { recipients, .. }) => recipients
            .iter()
            .map(|recipient| {
                (
                    recipient.row_id,
                    recipient.recipient_type,
                    serialize_pending_recipient_row(recipient),
                )
            })
            .collect::<Vec<_>>(),
        _ => return rop_error_response(0x0F, input_handle_index, 0x0000_04B9),
    };
    if available_rows.is_empty() {
        return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
    }
    let start_index = if start == 0 {
        0
    } else if let Some(index) = available_rows
        .iter()
        .position(|(row_id, _, _)| *row_id == start as u32)
    {
        index
    } else {
        return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
    };
    let mut row_count = 0usize;
    let mut rows = Vec::new();
    for (row_id, recipient_type, row) in available_rows
        .iter()
        .skip(start_index)
        .take(u8::MAX as usize)
    {
        write_u32(&mut rows, *row_id);
        rows.push(*recipient_type);
        rows.extend_from_slice(&0x0FFFu16.to_le_bytes());
        rows.extend_from_slice(&0u16.to_le_bytes());
        rows.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rows.extend_from_slice(row);
        row_count += 1;
    }
    response.push(row_count.min(u8::MAX as usize) as u8);
    response.extend_from_slice(&rows);
    response
}

pub(in crate::mapi) fn rop_set_message_read_flag_response(
    request: &RopRequest,
    read_status_changed: bool,
) -> Vec<u8> {
    let mut response = vec![0x11, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(read_status_changed as u8);
    response
}

pub(in crate::mapi) fn search_folder_message_for_id(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<&MapiMessage> {
    match folder_id {
        TODO_SEARCH_FOLDER_ID => snapshot.todo_search_message_for_id(message_id),
        TRACKED_MAIL_PROCESSING_FOLDER_ID => {
            snapshot.tracked_mail_processing_message_for_id(message_id)
        }
        REMINDERS_FOLDER_ID => snapshot.reminder_message_for_id(message_id),
        _ => None,
    }
}

pub(in crate::mapi) fn restriction_matches_mailbox_with_context_for_account(
    restriction: Option<&MapiRestriction>,
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    mailbox_guid: Uuid,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        mailbox_property_value_with_context_for_account(
            mailbox,
            mailboxes,
            property_tag,
            mailbox_guid,
        )
    })
}

pub(in crate::mapi) fn restriction_matches_collaboration_folder(
    restriction: Option<&MapiRestriction>,
    folder: &MapiCollaborationFolder,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        collaboration_folder_property_value(folder, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_public_folder(
    restriction: Option<&MapiRestriction>,
    folder: &MapiPublicFolder,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        public_folder_property_value(folder, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_email(
    restriction: Option<&MapiRestriction>,
    email: &JmapEmail,
) -> bool {
    restriction_matches_email_with_attachments(restriction, email, &[])
}

pub(in crate::mapi) fn restriction_matches_email_with_attachments(
    restriction: Option<&MapiRestriction>,
    email: &JmapEmail,
    attachments: &[MapiAttachment],
) -> bool {
    let Some(restriction) = restriction else {
        return true;
    };
    match restriction {
        MapiRestriction::InvalidTableRestriction => false,
        MapiRestriction::And(children) => children.iter().all(|child| {
            restriction_matches_email_with_attachments(Some(child), email, attachments)
        }),
        MapiRestriction::Or(children) => children.iter().any(|child| {
            restriction_matches_email_with_attachments(Some(child), email, attachments)
        }),
        MapiRestriction::Not(child) => {
            !restriction_matches_email_with_attachments(Some(child), email, attachments)
        }
        MapiRestriction::SubObject { subobject, child } => {
            match canonical_property_storage_tag(*subobject) {
                PID_TAG_MESSAGE_RECIPIENTS => message_recipients(email).iter().any(|recipient| {
                    restriction_matches(Some(child), |property_tag| {
                        recipient_property_value(recipient, property_tag)
                    })
                }),
                PID_TAG_MESSAGE_ATTACHMENTS => attachments
                    .iter()
                    .any(|attachment| restriction_matches_attachment(Some(child), attachment)),
                _ => false,
            }
        }
        MapiRestriction::Count { count, child } => {
            *count > 0
                && restriction_matches_email_with_attachments(Some(child), email, attachments)
        }
        MapiRestriction::Content {
            property_tag,
            value,
            fuzzy_level_low,
            fuzzy_level_high,
        } => email_property_value(email, *property_tag)
            .and_then(|property| property.into_text())
            .is_some_and(|property| {
                content_restriction_matches(&property, value, *fuzzy_level_low, *fuzzy_level_high)
            }),
        MapiRestriction::Property {
            relop,
            property_tag,
            value,
        } => email_property_value(email, *property_tag)
            .is_some_and(|property| compare_mapi_values(&property, value, *relop)),
        MapiRestriction::CompareProperties {
            relop,
            left_property_tag,
            right_property_tag,
        } => email_property_value(email, *left_property_tag).is_some_and(|left| {
            email_property_value(email, *right_property_tag)
                .is_some_and(|right| compare_mapi_values(&left, &right, *relop))
        }),
        MapiRestriction::Bitmask {
            property_tag,
            mask,
            must_be_nonzero,
        } => email_property_value(email, *property_tag)
            .and_then(|value| value.into_u32())
            .is_some_and(|value| ((value & mask) != 0) == *must_be_nonzero),
        MapiRestriction::Size {
            relop,
            property_tag,
            size,
        } => email_property_value(email, *property_tag)
            .map(|value| value.size() as i64)
            .is_some_and(|actual| compare_i64(actual, *size as i64, *relop)),
        MapiRestriction::Exist { property_tag } => {
            email_property_value(email, *property_tag).is_some()
        }
    }
}

fn recipient_property_value(recipient: &MapiRecipient<'_>, property_tag: u32) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let display_name = recipient
        .address
        .display_name
        .as_deref()
        .unwrap_or(&recipient.address.address);
    match property_tag {
        PID_TAG_RECIPIENT_TYPE => Some(MapiValue::U32(u32::from(recipient.recipient_type))),
        PID_TAG_RECIPIENT_ORDER => Some(MapiValue::U32(recipient.order)),
        PID_TAG_RECIPIENT_FLAGS => Some(MapiValue::U32(1)),
        PID_TAG_RECIPIENT_TRACK_STATUS => Some(MapiValue::U32(0)),
        PID_TAG_DISPLAY_NAME_W | PID_TAG_RECIPIENT_DISPLAY_NAME_W => {
            Some(MapiValue::String(display_name.to_string()))
        }
        PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
            Some(MapiValue::String(recipient.address.address.clone()))
        }
        PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W => {
            Some(MapiValue::String(display_name.to_string()))
        }
        0x3002_001F => Some(MapiValue::String("SMTP".to_string())),
        _ => None,
    }
}

pub(in crate::mapi) fn restriction_matches_contact_in_folder(
    restriction: Option<&MapiRestriction>,
    contact: &AccessibleContact,
    folder_id: u64,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        contact_property_value(contact, mapi_item_id(&contact.id), folder_id, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_task(
    restriction: Option<&MapiRestriction>,
    task: &ClientTask,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        task_property_value(task, mapi_item_id(&task.id), TASKS_FOLDER_ID, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_note(
    restriction: Option<&MapiRestriction>,
    note: &ClientNote,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        note_property_value(note, mapi_item_id(&note.id), NOTES_FOLDER_ID, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_journal_entry(
    restriction: Option<&MapiRestriction>,
    entry: &JournalEntry,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        journal_entry_property_value(
            entry,
            mapi_item_id(&entry.id),
            JOURNAL_FOLDER_ID,
            property_tag,
        )
    })
}

pub(in crate::mapi) fn restriction_matches_attachment(
    restriction: Option<&MapiRestriction>,
    attachment: &MapiAttachment,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        attachment_property_value(attachment, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_navigation_shortcut(
    restriction: Option<&MapiRestriction>,
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        navigation_shortcut_property_value(message, account_id, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_common_view_named_view(
    restriction: Option<&MapiRestriction>,
    message: &MapiCommonViewNamedViewMessage,
    account_id: Uuid,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        common_view_named_view_property_value(message, account_id, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches_associated_config(
    restriction: Option<&MapiRestriction>,
    message: &MapiAssociatedConfigMessage,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        associated_config_property_value(message, property_tag)
    })
}

pub(in crate::mapi) fn restriction_matches(
    restriction: Option<&MapiRestriction>,
    value_for: impl Copy + Fn(u32) -> Option<MapiValue>,
) -> bool {
    let Some(restriction) = restriction else {
        return true;
    };
    match restriction {
        MapiRestriction::InvalidTableRestriction => false,
        MapiRestriction::And(children) => children
            .iter()
            .all(|child| restriction_matches(Some(child), value_for)),
        MapiRestriction::Or(children) => children
            .iter()
            .any(|child| restriction_matches(Some(child), value_for)),
        MapiRestriction::Not(child) => !restriction_matches(Some(child), value_for),
        MapiRestriction::Content {
            property_tag,
            value,
            fuzzy_level_low,
            fuzzy_level_high,
        } => value_for(*property_tag)
            .and_then(|property| property.into_text())
            .is_some_and(|property| {
                content_restriction_matches(&property, value, *fuzzy_level_low, *fuzzy_level_high)
            }),
        MapiRestriction::Property {
            relop,
            property_tag,
            value,
        } => value_for(*property_tag)
            .is_some_and(|property| compare_mapi_values(&property, value, *relop)),
        MapiRestriction::CompareProperties {
            relop,
            left_property_tag,
            right_property_tag,
        } => value_for(*left_property_tag).is_some_and(|left| {
            value_for(*right_property_tag)
                .is_some_and(|right| compare_mapi_values(&left, &right, *relop))
        }),
        MapiRestriction::Bitmask {
            property_tag,
            mask,
            must_be_nonzero,
        } => value_for(*property_tag)
            .and_then(|value| value.into_u32())
            .is_some_and(|value| ((value & mask) != 0) == *must_be_nonzero),
        MapiRestriction::Size {
            relop,
            property_tag,
            size,
        } => value_for(*property_tag)
            .map(|value| value.size() as i64)
            .is_some_and(|actual| compare_i64(actual, *size as i64, *relop)),
        MapiRestriction::Exist { property_tag } => value_for(*property_tag).is_some(),
        MapiRestriction::Count { count, child } => {
            *count > 0 && restriction_matches(Some(child), value_for)
        }
        MapiRestriction::SubObject { .. } => false,
    }
}

fn content_restriction_matches(
    property: &str,
    value: &str,
    fuzzy_level_low: u16,
    fuzzy_level_high: u16,
) -> bool {
    let ignore_case = fuzzy_level_high & 0x0001 != 0 || fuzzy_level_high & 0x0004 != 0;
    let (property, value) = if ignore_case {
        (property.to_ascii_lowercase(), value.to_ascii_lowercase())
    } else {
        (property.to_string(), value.to_string())
    };

    match fuzzy_level_low {
        0x0000 => property == value,
        0x0002 => property.starts_with(&value),
        _ => property.contains(&value),
    }
}

#[cfg(test)]
pub(in crate::mapi) fn mailbox_property_value_with_context(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    property_tag: u32,
) -> Option<MapiValue> {
    mailbox_property_value_with_context_for_account(mailbox, mailboxes, property_tag, Uuid::nil())
}

pub(in crate::mapi) fn mailbox_property_value_with_context_for_account(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    property_tag: u32,
    mailbox_guid: Uuid,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    if matches!(mapi_folder_id(mailbox), ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
        if let Some(value) =
            special_folder_identification_property_value(mailbox_guid, property_tag)
        {
            return Some(value);
        }
    }
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(mapi_mailbox_display_name(mailbox))),
        PID_TAG_CONTENT_COUNT => Some(MapiValue::U32(mailbox.total_emails)),
        PID_TAG_CONTENT_UNREAD_COUNT => Some(MapiValue::U32(mailbox.unread_emails)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(mailbox.size_octets as i64)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => {
            Some(mapi_message_size_extended_value(mailbox.size_octets as i64))
        }
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(mailbox_has_subfolders(mailbox, mailboxes))),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(
            if mailbox.role == "__mapi_search" || mailbox.role.starts_with("__mapi_search_folder_")
            {
                FOLDER_SEARCH
            } else {
                FOLDER_GENERIC
            },
        )),
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(extended_folder_flags_for_folder(
            mapi_folder_id(mailbox),
        ))),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            if default_view_supported_folder(
                mapi_folder_id(mailbox),
                folder_message_class(mailbox),
            ) =>
        {
            default_folder_view_entry_id(
                mailbox_guid,
                mapi_folder_id(mailbox),
                folder_message_class(mailbox),
            )
        }
        PID_TAG_FOLDER_FORM_STORAGE => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_ATTRIBUTE_HIDDEN => {
            Some(MapiValue::Bool(mailbox_projects_hidden_attribute(mailbox)))
        }
        PID_TAG_CONTAINER_CLASS_W => Some(MapiValue::String(folder_message_class(mailbox).into())),
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(folder_message_class(mailbox))
                .map(|message_class| MapiValue::String(message_class.to_string()))
        }
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(mapi_folder_id(mailbox))),
        PID_TAG_ENTRY_ID => crate::mapi::identity::folder_entry_id_from_object_id(
            mailbox_guid,
            mapi_folder_id(mailbox),
        )
        .map(MapiValue::Binary),
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_mailbox_folder(mailbox),
        )),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(mapi_folder_id(mailbox)),
        )),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
            mapi_mailstore::canonical_folder_change_number(mailbox),
        ))),
        PID_TAG_DELETED_COUNT_TOTAL => Some(MapiValue::U32(0)),
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => Some(MapiValue::U32(
            mapi_mailstore::canonical_folder_change_number(mailbox).min(u64::from(u32::MAX)) as u32,
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_mailbox_folder(mailbox),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(mailbox_parent_folder_id(mailbox, mailboxes)),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::canonical_folder_change_number(mailbox),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::canonical_folder_change_number(mailbox),
            )))
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(
            mapi_mailstore::canonical_folder_change_number(mailbox),
        )),
        _ => None,
    }
}

pub(in crate::mapi) fn mapi_mailbox_display_name(mailbox: &JmapMailbox) -> String {
    if mailbox.role.eq_ignore_ascii_case("inbox") {
        "Inbox".to_string()
    } else if mailbox.role == "conversation_history" {
        "Conversation History".to_string()
    } else {
        mailbox.name.clone()
    }
}

pub(in crate::mapi) fn default_post_message_class_for_container_class(
    container_class: &str,
) -> Option<&'static str> {
    match container_class {
        "IPF.Note" => Some("IPM.Note"),
        class if class.starts_with("IPF.Note.") => Some("IPM.Note"),
        "IPF.Appointment" => Some("IPM.Appointment"),
        "IPF.Contact" | "IPF.Contact.MOC.QuickContacts" | "IPF.Contact.MOC.ImContactList" => {
            Some("IPM.Contact")
        }
        "IPF.Task" => Some("IPM.Task"),
        "IPF.StickyNote" => Some("IPM.StickyNote"),
        "IPF.Journal" => Some("IPM.Activity"),
        "IPF.Configuration" => Some("IPM.Configuration"),
        "Outlook.Reminder" => Some("IPM.Note"),
        _ => None,
    }
}

pub(in crate::mapi) fn extended_folder_flags() -> Vec<u8> {
    vec![0x01, 0x04, 0x00, 0x00, 0x10, 0x00]
}

pub(in crate::mapi) fn extended_folder_flags_for_folder(folder_id: u64) -> Vec<u8> {
    let mut flags = extended_folder_flags();
    if folder_id == TODO_SEARCH_FOLDER_ID {
        flags.extend_from_slice(&[0x05, 0x04]);
        flags.extend_from_slice(&0x000C_0000u32.to_le_bytes());
    }
    flags
}

fn extended_folder_flags_for_search_folder(
    definition: &SearchFolderDefinition,
    folder_id: u64,
) -> Vec<u8> {
    let mut flags = extended_folder_flags_for_folder(folder_id);
    flags.extend_from_slice(&[0x03, 0x04]);
    flags.extend_from_slice(&search_folder_tag(definition).to_le_bytes());
    flags.extend_from_slice(&[0x02, 0x10]);
    flags.extend_from_slice(&search_folder_id(definition));
    flags
}

fn search_folder_id(definition: &SearchFolderDefinition) -> Vec<u8> {
    definition.id.as_bytes().to_vec()
}

fn mailbox_has_subfolders(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> bool {
    if mapi_folder_id(mailbox) == SYNC_ISSUES_FOLDER_ID {
        return false;
    }
    !mailboxes.is_empty()
        && mailboxes
            .iter()
            .any(|candidate| candidate.parent_id == Some(mailbox.id))
}

fn mailbox_parent_folder_id(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> u64 {
    match mapi_folder_id(mailbox) {
        IPM_SUBTREE_FOLDER_ID
        | DEFERRED_ACTION_FOLDER_ID
        | SPOOLER_QUEUE_FOLDER_ID
        | COMMON_VIEWS_FOLDER_ID
        | SCHEDULE_FOLDER_ID
        | SEARCH_FOLDER_ID
        | VIEWS_FOLDER_ID
        | SHORTCUTS_FOLDER_ID
        | REMINDERS_FOLDER_ID
        | DOCUMENT_LIBRARIES_FOLDER_ID
        | TRACKED_MAIL_PROCESSING_FOLDER_ID
        | TODO_SEARCH_FOLDER_ID
        | FREEBUSY_DATA_FOLDER_ID => return ROOT_FOLDER_ID,
        _ => {}
    }
    match mailbox.role.as_str() {
        "conflicts" | "local_failures" | "server_failures" => SYNC_ISSUES_FOLDER_ID,
        _ => mailbox
            .parent_id
            .and_then(|parent_id| mailboxes.iter().find(|candidate| candidate.id == parent_id))
            .map(|parent| mapi_folder_id(parent))
            .unwrap_or(IPM_SUBTREE_FOLDER_ID),
    }
}

pub(in crate::mapi) fn collaboration_folder_property_value(
    folder: &MapiCollaborationFolder,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(folder.id);
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(folder.collection.display_name.clone())),
        PID_TAG_CONTENT_COUNT => Some(MapiValue::U32(folder.item_count)),
        PID_TAG_CONTENT_UNREAD_COUNT => Some(MapiValue::U32(0)),
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(false)),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_GENERIC)),
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(extended_folder_flags())),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
        PID_TAG_FOLDER_FORM_STORAGE => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_CONTAINER_CLASS_W => Some(MapiValue::String(
            collaboration_folder_message_class(folder.kind).to_string(),
        )),
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(collaboration_folder_message_class(
                folder.kind,
            ))
            .map(|message_class| MapiValue::String(message_class.to_string()))
        }
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            if default_view_supported_folder(
                folder.id,
                collaboration_folder_message_class(folder.kind),
            ) =>
        {
            default_folder_view_entry_id(
                folder.collection.owner_account_id,
                folder.id,
                collaboration_folder_message_class(folder.kind),
            )
        }
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder.id)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(IPM_SUBTREE_FOLDER_ID)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::folder_entry_id_from_object_id(
            folder.collection.owner_account_id,
            folder.id,
        )
        .map(MapiValue::Binary),
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder.id,
        ))),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(folder.id),
        )),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
            change_number,
        ))),
        PID_TAG_DELETED_COUNT_TOTAL => Some(MapiValue::U32(0)),
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => {
            Some(MapiValue::U32(change_number.min(u64::from(u32::MAX)) as u32))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            collaboration_folder_message_class(folder.kind).to_string(),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

pub(in crate::mapi) fn public_folder_property_value(
    folder: &MapiPublicFolder,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(folder.id);
    let parent_folder_id = folder
        .folder
        .parent_folder_id
        .and_then(|parent_id| crate::mapi::identity::mapped_mapi_object_id(&parent_id))
        .unwrap_or(PUBLIC_FOLDERS_ROOT_FOLDER_ID);
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(folder.folder.display_name.clone())),
        PID_TAG_CONTENT_COUNT => Some(MapiValue::U32(folder.item_count)),
        PID_TAG_CONTENT_UNREAD_COUNT => Some(MapiValue::U32(0)),
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(folder.child_count > 0)),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_GENERIC)),
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(extended_folder_flags())),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_FOLDER_FORM_FLAGS | PID_TAG_FOLDER_VIEWS_ONLY | PID_TAG_FOLDER_VIEWLIST_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_FORM_NAME_W => Some(MapiValue::String(String::new())),
        PID_TAG_FOLDER_FORM_STORAGE => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W => {
            Some(MapiValue::String(folder.folder.folder_class.clone()))
        }
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(&folder.folder.folder_class)
                .map(|message_class| MapiValue::String(message_class.to_string()))
        }
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder.id)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(parent_folder_id)),
        PID_TAG_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(Uuid::nil(), folder.id)
                .map(MapiValue::Binary)
        }
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder.id,
        ))),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(folder.id),
        )),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
            change_number,
        ))),
        PID_TAG_DELETED_COUNT_TOTAL => Some(MapiValue::U32(0)),
        PID_TAG_SERIALIZED_REPLID_GUID_MAP => Some(MapiValue::Binary(serialized_replid_guid_map())),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => {
            Some(MapiValue::U32(change_number.min(u64::from(u32::MAX)) as u32))
        }
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(parent_folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

pub(in crate::mapi) fn navigation_shortcut_property_value(
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    navigation_shortcut_property_value_with_store_entry_id(message, account_id, None, property_tag)
}

pub(in crate::mapi) fn navigation_shortcut_property_value_for_principal(
    message: &MapiNavigationShortcutMessage,
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Option<MapiValue> {
    let store_entry_id = super::identity::principal_mailbox_store_entry_id(principal);
    navigation_shortcut_property_value_with_store_entry_id(
        message,
        principal.account_id,
        Some(&store_entry_id),
        property_tag,
    )
}

fn navigation_shortcut_property_value_with_store_entry_id(
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
    store_entry_id: Option<&[u8]>,
    property_tag: u32,
) -> Option<MapiValue> {
    let requested_property_tag = property_tag;
    let property_tag = canonical_property_storage_tag(property_tag);
    match property_tag {
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_MID => Some(MapiValue::U64(message.id)),
        PID_TAG_INST_ID => Some(MapiValue::U64(message.id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            message.folder_id,
            message.id,
        )
        .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(message.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            "IPM.Microsoft.WunderBar.Link".to_string(),
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_FAI)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(128)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(MapiValue::I64(128)),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            message.id,
        ))),
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            message.id,
        ))),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::change_number_for_store_id(message.id),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::change_number_for_store_id(message.id),
            )))
        }
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_PARENT_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(account_id, message.folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(message.id & 0x00FF_FFFF_FFFF_FFFF)),
        PID_TAG_WLINK_SAVE_STAMP => Some(MapiValue::U32(wlink_save_stamp(message))),
        PID_TAG_WLINK_TYPE => Some(MapiValue::U32(message.shortcut_type)),
        PID_TAG_WLINK_FLAGS => Some(MapiValue::U32(message.flags)),
        PID_TAG_WLINK_SECTION => Some(MapiValue::U32(message.section)),
        PID_TAG_WLINK_ORDINAL => Some(MapiValue::Binary(wlink_ordinal_bytes(message.ordinal))),
        property_tag
            if property_tag_id(property_tag) == property_tag_id(PID_TAG_WLINK_GROUP_HEADER_ID) =>
        {
            let group_id = message
                .group_header_id
                .map(|group_id| *group_id.as_bytes())
                .unwrap_or_else(default_wlink_group_guid);
            Some(wlink_guid_property_value(requested_property_tag, group_id))
        }
        property_tag
            if property_tag_id(property_tag) == property_tag_id(PID_TAG_WLINK_GROUP_CLSID) =>
        {
            let group_id = message
                .group_header_id
                .map(|group_id| *group_id.as_bytes())
                .unwrap_or_else(default_wlink_group_guid);
            Some(wlink_guid_property_value(requested_property_tag, group_id))
        }
        PID_TAG_WLINK_GROUP_NAME_W => Some(MapiValue::String(wlink_group_name(message))),
        PID_TAG_WLINK_ENTRY_ID if message.shortcut_type != 4 => message
            .target_folder_id
            .and_then(|folder_id| {
                crate::mapi::identity::folder_entry_id_from_object_id(account_id, folder_id)
            })
            .map(MapiValue::Binary),
        property_tag
            if is_sharing_local_folder_id_property_tag(property_tag)
                && message.shortcut_type != 4 =>
        {
            message
                .target_folder_id
                .and_then(|folder_id| {
                    crate::mapi::identity::folder_entry_id_from_object_id(account_id, folder_id)
                })
                .map(MapiValue::Binary)
        }
        PID_TAG_WLINK_RECORD_KEY if message.shortcut_type != 4 => message
            .target_folder_id
            .map(mapi_mailstore::source_key_for_store_id)
            .map(MapiValue::Binary),
        PID_TAG_WLINK_STORE_ENTRY_ID if message.shortcut_type != 4 => {
            store_entry_id.map(|value| MapiValue::Binary(value.to_vec()))
        }
        PID_TAG_WLINK_CALENDAR_COLOR if navigation_shortcut_targets_calendar(message) => {
            Some(MapiValue::I32(-1))
        }
        // [MS-OXOCFG] sections 2.2.9.16, 2.2.9.18, and 3.1.3.1 make
        // these optional values client-owned. A server-created shortcut cannot
        // synthesize either the owner's NSPI EntryID or an Outlook Client ID.
        PID_TAG_WLINK_ADDRESS_BOOK_EID | PID_TAG_WLINK_CLIENT_ID
            if navigation_shortcut_targets_calendar(message) =>
        {
            None
        }
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID if message.shortcut_type != 4 => {
            store_entry_id.map(|value| MapiValue::Binary(value.to_vec()))
        }
        PID_TAG_WLINK_RO_GROUP_TYPE if navigation_shortcut_targets_calendar(message) => {
            Some(MapiValue::I32(-1))
        }
        property_tag
            if property_tag_id(property_tag) == property_tag_id(PID_TAG_WLINK_FOLDER_TYPE) =>
        {
            Some(wlink_guid_property_value(
                requested_property_tag,
                wlink_folder_type_guid(message),
            ))
        }
        _ => None,
    }
}

fn navigation_shortcut_targets_calendar(message: &MapiNavigationShortcutMessage) -> bool {
    message.shortcut_type != 4
        && (message.section == 3 || message.target_folder_id == Some(CALENDAR_FOLDER_ID))
}

fn is_sharing_local_folder_id_property_tag(property_tag: u32) -> bool {
    matches!(
        property_tag,
        PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG
            | OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG
    )
}

pub(in crate::mapi) fn common_view_named_view_property_value(
    message: &MapiCommonViewNamedViewMessage,
    account_id: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    let requested_property_tag = property_tag;
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(message.id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            message.folder_id,
            message.id,
        )
        .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(message.name.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            "IPM.Microsoft.FolderDesign.NamedView".to_string(),
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_FAI)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(128)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(MapiValue::I64(128)),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_SOURCE_KEY | PID_TAG_RECORD_KEY | PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.id),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_PARENT_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(account_id, message.folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::change_number_for_store_id(message.id),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::change_number_for_store_id(message.id),
            )))
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(mapi_mailstore::change_number_for_store_id(
            message.id,
        ))),
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => {
            Some(MapiValue::U64(mapi_mailstore::filetime_from_change_number(
                mapi_mailstore::change_number_for_store_id(message.id),
            )))
        }
        PID_TAG_VIEW_DESCRIPTOR_FLAGS => Some(MapiValue::U32(message.view_flags)),
        PID_TAG_VIEW_DESCRIPTOR_BINARY | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835 => {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            log_view_definition_diagnostics(
                message.folder_id,
                message.id,
                &message.name,
                &definition,
            );
            Some(MapiValue::Binary(view_descriptor_binary(&definition)))
        }
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C => {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            log_view_definition_diagnostics(
                message.folder_id,
                message.id,
                &message.name,
                &definition,
            );
            Some(MapiValue::Binary(view_descriptor_strings_binary(
                &definition,
            )))
        }
        PID_TAG_VIEW_DESCRIPTOR_VERSION | PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL => {
            Some(MapiValue::U32(message.view_type))
        }
        PID_TAG_VIEW_DESCRIPTOR_NAME_W => Some(MapiValue::String(message.name.clone())),
        PID_TAG_VIEW_DESCRIPTOR_STRINGS_W => {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            log_view_definition_diagnostics(
                message.folder_id,
                message.id,
                &message.name,
                &definition,
            );
            Some(MapiValue::String(view_descriptor_strings(&definition)))
        }
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE => Some(MapiValue::U32(0)),
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B => {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            log_view_definition_diagnostics(
                message.folder_id,
                message.id,
                &message.name,
                &definition,
            );
            Some(MapiValue::Binary(view_descriptor_binary(&definition)))
        }
        tag if property_tag_id(tag) == property_tag_id(PID_TAG_VIEW_DESCRIPTOR_CLSID) => Some(
            wlink_guid_property_value(requested_property_tag, *message.canonical_id.as_bytes()),
        ),
        tag if property_tag_id(tag) == property_tag_id(PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE) => {
            Some(wlink_guid_property_value(
                requested_property_tag,
                common_view_named_view_folder_type_guid(),
            ))
        }
        tag if property_tag_id(tag) == property_tag_id(PID_TAG_WLINK_GROUP_HEADER_ID) => Some(
            wlink_guid_property_value(requested_property_tag, default_wlink_group_guid()),
        ),
        _ => None,
    }
}

fn property_tag_id(property_tag: u32) -> u32 {
    property_tag & 0xFFFF_0000
}

fn named_property_id_matches(left: u32, right: u32) -> bool {
    property_tag_id(left) == property_tag_id(right)
}

fn wlink_guid_property_value(property_tag: u32, guid: [u8; 16]) -> MapiValue {
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::Binary) => MapiValue::Binary(guid.to_vec()),
        _ => MapiValue::Guid(guid),
    }
}

pub(in crate::mapi) fn default_wlink_group_guid() -> [u8; 16] {
    [
        0x5B, 0xA9, 0x43, 0xD8, 0xDA, 0xAA, 0x46, 0x2C, 0xA6, 0x3E, 0x91, 0x36, 0xF6, 0x5C, 0x86,
        0x81,
    ]
}

pub(crate) fn default_wlink_group_uuid() -> Uuid {
    Uuid::from_bytes(default_wlink_group_guid())
}

fn wlink_group_name(message: &MapiNavigationShortcutMessage) -> String {
    if message.section == 1
        && message.group_header_id == Some(default_wlink_group_uuid())
        && (message.group_name.trim().is_empty()
            || message.group_name.eq_ignore_ascii_case("Mail")
            || message.group_name.eq_ignore_ascii_case("Favorites"))
    {
        return "Favorites".to_string();
    }
    if message.group_name.trim().is_empty() {
        "Mail".to_string()
    } else {
        message.group_name.clone()
    }
}

fn wlink_save_stamp(message: &MapiNavigationShortcutMessage) -> u32 {
    if message.save_stamp != 0 {
        return message.save_stamp;
    }
    let bytes = message
        .group_header_id
        .as_ref()
        .map(Uuid::as_bytes)
        .unwrap_or_else(|| message.canonical_id.as_bytes());
    let stamp = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if stamp == 0 {
        1
    } else {
        stamp
    }
}

fn wlink_mail_folder_type_guid() -> [u8; 16] {
    [
        0x0C, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ]
}

pub(in crate::mapi) fn common_view_named_view_folder_type_guid() -> [u8; 16] {
    [
        0x00, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ]
}

pub(in crate::mapi) fn wlink_folder_type_guid(message: &MapiNavigationShortcutMessage) -> [u8; 16] {
    if message.target_folder_id.is_some_and(|folder_id| {
        matches!(
            folder_id,
            INBOX_FOLDER_ID
                | OUTBOX_FOLDER_ID
                | SENT_FOLDER_ID
                | DRAFTS_FOLDER_ID
                | TRASH_FOLDER_ID
                | JUNK_FOLDER_ID
                | ARCHIVE_FOLDER_ID
        )
    }) {
        return wlink_mail_folder_type_guid();
    }
    [
        0x02, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ]
}

pub(in crate::mapi) fn wlink_ordinal_bytes(value: u32) -> Vec<u8> {
    let mut bytes = if value <= u8::MAX as u32 {
        vec![value as u8]
    } else {
        value
            .to_be_bytes()
            .into_iter()
            .skip_while(|byte| *byte == 0)
            .collect()
    };
    match bytes.last_mut() {
        Some(last) if *last == 0 => *last = 1,
        Some(last) if *last == u8::MAX => *last = u8::MAX - 1,
        None => bytes.push(1),
        _ => {}
    }
    bytes
}

pub(in crate::mapi) fn conversation_action_property_value(
    message: &MapiConversationActionMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let action = &message.action;
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(message.id)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
            Uuid::nil(),
            message.folder_id,
            message.id,
        )
        .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_CONVERSATION_TOPIC_W => {
            Some(MapiValue::String(conversation_action_subject(action)))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.ConversationAction".to_string())),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(
            conversation_action_size(action).min(i32::MAX as usize) as i32,
        )),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
            conversation_action_size(action).min(i64::MAX as usize) as i64,
        )),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_CONVERSATION_INDEX => Some(MapiValue::Binary(conversation_index_for_uuid(
            action.conversation_id,
        ))),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            message.id,
        ))),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::change_number_for_store_id(message.id),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::change_number_for_store_id(message.id),
            )))
        }
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(message.id & 0x00FF_FFFF_FFFF_FFFF)),
        PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG => {
            action.move_folder_entry_id.clone().map(MapiValue::Binary)
        }
        PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG => {
            action.move_store_entry_id.clone().map(MapiValue::Binary)
        }
        PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG => action
            .max_delivery_time
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG => action
            .last_applied_time
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_CONVERSATION_ACTION_VERSION_TAG => Some(MapiValue::I32(action.version)),
        PID_LID_CONVERSATION_PROCESSED_TAG => Some(MapiValue::I32(action.processed)),
        PID_NAME_KEYWORDS_TAG => Some(MapiValue::MultiString(json_string_array(
            &action.categories_json,
        ))),
        _ => None,
    }
}

pub(in crate::mapi) fn compare_mapi_values(left: &MapiValue, right: &MapiValue, relop: u8) -> bool {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return compare_i64(left, right, relop);
    }
    if let (Some(left), Some(right)) = (left.as_text(), right.as_text()) {
        return compare_ordering(compare_case_insensitive(left, right), relop);
    }
    if let (Some(left), Some(right)) = (left.as_bool(), right.as_bool()) {
        return compare_ordering(left.cmp(&right), relop);
    }
    if let Some(ordering) = compare_folder_entry_id_values(left, right) {
        return compare_ordering(ordering, relop);
    }
    compare_ordering(left.cmp_value(right), relop)
}

fn compare_folder_entry_id_values(left: &MapiValue, right: &MapiValue) -> Option<Ordering> {
    let (MapiValue::Binary(left), MapiValue::Binary(right)) = (left, right) else {
        return None;
    };
    let left = crate::mapi::identity::object_id_from_folder_entry_id(left)?;
    let right = crate::mapi::identity::object_id_from_folder_entry_id(right)?;
    Some(left.cmp(&right))
}

pub(in crate::mapi) fn compare_i64(left: i64, right: i64, relop: u8) -> bool {
    compare_ordering(left.cmp(&right), relop)
}

pub(in crate::mapi) fn compare_ordering(ordering: Ordering, relop: u8) -> bool {
    match relop {
        0x00 => ordering == Ordering::Less,
        0x01 => matches!(ordering, Ordering::Less | Ordering::Equal),
        0x02 => ordering == Ordering::Greater,
        0x03 => matches!(ordering, Ordering::Greater | Ordering::Equal),
        0x04 => ordering == Ordering::Equal,
        0x05 => ordering != Ordering::Equal,
        _ => false,
    }
}

pub(in crate::mapi) fn default_mapping_rights() -> CollaborationRights {
    CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: false,
    }
}

pub(in crate::mapi) fn apply_mapi_property_values(
    object: Option<&mut MapiObject>,
    values: Vec<(u32, MapiValue)>,
) -> Result<()> {
    let values = values
        .into_iter()
        .map(|(tag, value)| (canonical_property_storage_tag(tag), value))
        .collect::<Vec<_>>();
    match object {
        Some(MapiObject::PendingMessage { properties, .. }) => {
            properties.extend(values);
            Ok(())
        }
        Some(MapiObject::PendingAssociatedMessage { properties, .. })
        | Some(MapiObject::PendingContact { properties, .. })
        | Some(MapiObject::PendingEvent { properties, .. })
        | Some(MapiObject::PendingTask { properties, .. })
        | Some(MapiObject::PendingNote { properties, .. })
        | Some(MapiObject::PendingJournalEntry { properties, .. })
        | Some(MapiObject::PendingConversationAction { properties, .. })
        | Some(MapiObject::PendingNavigationShortcut { properties, .. }) => {
            properties.extend(values);
            Ok(())
        }
        Some(MapiObject::PendingAttachment {
            properties, data, ..
        }) => {
            for (tag, value) in values {
                if tag == PID_TAG_ATTACH_DATA_BINARY {
                    if let MapiValue::Binary(bytes) = &value {
                        *data = bytes.clone();
                    }
                }
                properties.insert(tag, value);
            }
            Ok(())
        }
        Some(MapiObject::Folder {
            folder_id,
            properties,
        }) => {
            properties.extend(values.into_iter().filter(|(tag, _)| {
                *folder_id != ROOT_FOLDER_ID
                    || !is_default_folder_identification_property_tag(*tag)
                    || is_scalar_default_folder_entry_id_property_tag(*tag)
            }));
            Ok(())
        }
        Some(MapiObject::Logon | MapiObject::PublicFolderLogon) => Ok(()),
        _ => Err(anyhow!("MAPI object does not support property mutation")),
    }
}

pub(in crate::mapi) fn delete_mapi_properties(
    object: Option<&mut MapiObject>,
    property_tags: &[u32],
) -> Result<()> {
    let property_tags = property_tags
        .iter()
        .flat_map(|tag| [*tag, canonical_property_storage_tag(*tag)])
        .collect::<Vec<_>>();
    match object {
        Some(MapiObject::PendingMessage { properties, .. }) => {
            for tag in &property_tags {
                properties.remove(tag);
            }
            Ok(())
        }
        Some(MapiObject::PendingAssociatedMessage { properties, .. })
        | Some(MapiObject::PendingContact { properties, .. })
        | Some(MapiObject::PendingEvent { properties, .. })
        | Some(MapiObject::PendingTask { properties, .. })
        | Some(MapiObject::PendingNote { properties, .. })
        | Some(MapiObject::PendingJournalEntry { properties, .. })
        | Some(MapiObject::PendingConversationAction { properties, .. })
        | Some(MapiObject::PendingNavigationShortcut { properties, .. }) => {
            for tag in &property_tags {
                properties.remove(tag);
            }
            Ok(())
        }
        Some(MapiObject::PendingAttachment {
            properties, data, ..
        }) => {
            for tag in &property_tags {
                properties.remove(tag);
                if *tag == PID_TAG_ATTACH_DATA_BINARY {
                    data.clear();
                }
            }
            Ok(())
        }
        Some(MapiObject::Folder { properties, .. }) => {
            for tag in &property_tags {
                properties.remove(tag);
            }
            Ok(())
        }
        _ => Err(anyhow!("MAPI object does not support property deletion")),
    }
}

#[cfg(test)]
mod tests;
