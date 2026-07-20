use crate::mapi::properties::MapiValue;
use crate::mapi::rop::utf16le_bytes;
use crate::mapi::session::MapiObject;
pub(in crate::mapi) use lpe_domain::crypto::hex_lower as format_bytes_hex;
use lpe_domain::crypto::{hex_lower, sha256_hex_prefix};

pub(in crate::mapi) fn view_descriptor_value_shape_for_debug(value: &MapiValue) -> String {
    match value {
        MapiValue::Binary(bytes) => format!(
            "binary_bytes={};sha256_16={};preview={}",
            bytes.len(),
            sha256_hex_prefix(bytes, 16),
            hex_preview_for_debug(bytes, 64)
        ),
        MapiValue::String(value) => {
            let bytes = utf16le_bytes(value);
            format!(
                "string_chars={};utf16_bytes={};sha256_16={};preview={}",
                value.chars().count(),
                bytes.len(),
                sha256_hex_prefix(&bytes, 16),
                text_preview_for_debug(value, 48)
            )
        }
        MapiValue::U32(value) => format!("u32={value}"),
        value => mapi_value_shape_for_debug(value),
    }
}

pub(in crate::mapi) fn mapi_value_shape_for_debug(value: &MapiValue) -> String {
    match value {
        MapiValue::Null => "null".to_string(),
        MapiValue::Bool(value) => format!("bool={value}"),
        MapiValue::I16(value) => format!("i16={value}"),
        MapiValue::I32(value) => format!("i32={value}"),
        MapiValue::I64(value) => format!("i64={value}"),
        MapiValue::F64(value) => format!("f64={}", f64::from_bits(*value)),
        MapiValue::U32(value) => format!("u32={value}"),
        MapiValue::U64(value) => format!("u64={value}"),
        MapiValue::String(value) => format!(
            "string:chars={}:preview={}",
            value.chars().count(),
            text_preview_for_debug(value, 32)
        ),
        MapiValue::Binary(value) => {
            format!(
                "binary:bytes={}:preview={}",
                value.len(),
                hex_preview_for_debug(value, 16)
            )
        }
        MapiValue::Guid(value) => format!("guid={}", hex_preview_for_debug(value, value.len())),
        MapiValue::Error(value) => format!("error={value:#010x}"),
        MapiValue::MultiI16(values) => format!("multi_i16:count={}", values.len()),
        MapiValue::MultiI32(values) => format!("multi_i32:count={}", values.len()),
        MapiValue::MultiI64(values) => format!("multi_i64:count={}", values.len()),
        MapiValue::MultiString(values) => format!("multi_string:count={}", values.len()),
        MapiValue::MultiBinary(values) => format!("multi_binary:count={}", values.len()),
        MapiValue::MultiGuid(values) => format!("multi_guid:count={}", values.len()),
    }
}

pub(in crate::mapi) fn text_preview_for_debug(value: &str, max_chars: usize) -> String {
    value
        .chars()
        .take(max_chars)
        .map(|ch| match ch {
            ',' | ';' | '\n' | '\r' | '\t' => ' ',
            _ => ch,
        })
        .collect()
}

pub(in crate::mapi) fn mapi_object_debug_fields(
    object: Option<&MapiObject>,
) -> (&'static str, String, String) {
    match object {
        Some(MapiObject::Logon) => ("logon", String::new(), String::new()),
        Some(MapiObject::PublicFolderLogon) => {
            ("public_folder_logon", String::new(), String::new())
        }
        Some(MapiObject::Folder { folder_id, .. }) => {
            ("folder", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::Message {
            folder_id,
            message_id,
            ..
        }) => (
            "message",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}"),
        ),
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
        }) => (
            "contact",
            format!("{folder_id:#018x}"),
            format!("{contact_id:#018x}"),
        ),
        Some(MapiObject::Event {
            folder_id,
            event_id,
            ..
        }) => (
            "event",
            format!("{folder_id:#018x}"),
            format!("{event_id:#018x}"),
        ),
        Some(MapiObject::Task { folder_id, task_id }) => (
            "task",
            format!("{folder_id:#018x}"),
            format!("{task_id:#018x}"),
        ),
        Some(MapiObject::Note { folder_id, note_id }) => (
            "note",
            format!("{folder_id:#018x}"),
            format!("{note_id:#018x}"),
        ),
        Some(MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        }) => (
            "journal_entry",
            format!("{folder_id:#018x}"),
            format!("{journal_entry_id:#018x}"),
        ),
        Some(MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
        }) => (
            "conversation_action",
            format!("{folder_id:#018x}"),
            format!("{conversation_action_id:#018x}"),
        ),
        Some(MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
            ..
        }) => (
            "navigation_shortcut",
            format!("{folder_id:#018x}"),
            format!("{shortcut_id:#018x}"),
        ),
        Some(MapiObject::CommonViewNamedView { folder_id, view_id }) => (
            "common_view_named_view",
            format!("{folder_id:#018x}"),
            format!("{view_id:#018x}"),
        ),
        Some(MapiObject::SearchFolderDefinitionMessage {
            folder_id,
            message_id,
        }) => (
            "search_folder_definition_message",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}"),
        ),
        Some(MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            ..
        }) => (
            "associated_config",
            format!("{folder_id:#018x}"),
            format!("{config_id:#018x}"),
        ),
        Some(MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
            ..
        }) => (
            "delegate_freebusy_message",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}"),
        ),
        Some(MapiObject::RecoverableItem { folder_id, item_id }) => (
            "recoverable_item",
            format!("{folder_id:#018x}"),
            format!("{item_id:#018x}"),
        ),
        Some(MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        }) => (
            "public_folder_item",
            format!("{folder_id:#018x}"),
            format!("{item_id:#018x}"),
        ),
        Some(MapiObject::PendingMessage { folder_id, .. }) => (
            "pending_message",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingAssociatedMessage { folder_id, .. }) => (
            "pending_associated_message",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingContact { folder_id, .. }) => (
            "pending_contact",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingEvent { folder_id, .. }) => {
            ("pending_event", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::PendingTask { folder_id, .. }) => {
            ("pending_task", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::PendingNote { folder_id, .. }) => {
            ("pending_note", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::PendingJournalEntry { folder_id, .. }) => (
            "pending_journal_entry",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingConversationAction { folder_id, .. }) => (
            "pending_conversation_action",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::PendingNavigationShortcut { folder_id, .. }) => (
            "pending_navigation_shortcut",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::HierarchyTable { folder_id, .. }) => (
            "hierarchy_table",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::ContentsTable { folder_id, .. }) => (
            "contents_table",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::AttachmentTable {
            folder_id,
            message_id,
            ..
        }) => (
            "attachment_table",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}"),
        ),
        Some(MapiObject::PermissionTable { folder_id, .. }) => (
            "permission_table",
            format!("{folder_id:#018x}"),
            String::new(),
        ),
        Some(MapiObject::RuleTable { folder_id, .. }) => {
            ("rule_table", format!("{folder_id:#018x}"), String::new())
        }
        Some(MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        }) => (
            "attachment",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}/{}", attach_num),
        ),
        Some(MapiObject::PendingAttachment {
            folder_id,
            message_id,
            attach_num,
            ..
        }) => (
            "pending_attachment",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}/{}", attach_num),
        ),
        Some(MapiObject::SavedAttachment {
            folder_id,
            message_id,
            attach_num,
            ..
        }) => (
            "saved_attachment",
            format!("{folder_id:#018x}"),
            format!("{message_id:#018x}/{}", attach_num),
        ),
        Some(MapiObject::AttachmentStream { .. }) => {
            ("attachment_stream", String::new(), String::new())
        }
        Some(MapiObject::NotificationSubscription { .. }) => {
            ("notification_subscription", String::new(), String::new())
        }
        Some(MapiObject::SynchronizationSource {
            folder_id,
            sync_type,
            ..
        }) => (
            "synchronization_source",
            format!("{folder_id:#018x}"),
            format!("{sync_type:#04x}"),
        ),
        Some(MapiObject::SynchronizationCollector {
            folder_id,
            checkpoint_kind,
            ..
        }) => (
            "synchronization_collector",
            format!("{folder_id:#018x}"),
            format!("{checkpoint_kind:?}"),
        ),
        Some(MapiObject::FastTransferDestination {
            folder_id,
            target_handle,
            ..
        }) => (
            "fast_transfer_destination",
            format!("{folder_id:#018x}"),
            format!("target_handle={target_handle}"),
        ),
        None => ("unknown", String::new(), String::new()),
    }
}

pub(in crate::mapi) fn hex_preview_for_debug(bytes: &[u8], max_bytes: usize) -> String {
    let mut preview = hex_lower(&bytes[..bytes.len().min(max_bytes)]);
    if bytes.len() > max_bytes {
        preview.push_str("...");
    }
    preview
}
