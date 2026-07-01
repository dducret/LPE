use super::*;
use lpe_domain::crypto::hex_lower;

pub(super) fn debug_folder_row_property_value<F>(
    value: F,
    folder_id: u64,
    parent_folder_id: u64,
    property_tag: u32,
    mailbox_guid: Uuid,
    associated_count: u32,
) -> Option<MapiValue>
where
    F: FnOnce() -> Option<MapiValue>,
{
    value().or_else(|| match canonical_property_storage_tag(property_tag) {
        PID_TAG_PARENT_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, parent_folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_ASSOCIATED_CONTENT_COUNT => Some(MapiValue::U32(associated_count)),
        PID_TAG_RECORD_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder_id,
        ))),
        _ => None,
    })
}

pub(super) fn classify_outlook_bootstrap_row_invariants<F>(
    row_index: usize,
    row_kind: &str,
    object_id: u64,
    expected_folder_id: Option<u64>,
    expected_parent_id: Option<u64>,
    expected_container_class: Option<&str>,
    mut value: F,
) -> String
where
    F: FnMut(u32) -> Option<MapiValue>,
{
    let entry_id = binary_property(value(PID_TAG_ENTRY_ID));
    let record_key = binary_property(value(PID_TAG_RECORD_KEY));
    let source_key = binary_property(value(PID_TAG_SOURCE_KEY));
    let parent_source_key = binary_property(value(PID_TAG_PARENT_SOURCE_KEY));
    let parent_entry_id = binary_property(value(PID_TAG_PARENT_ENTRY_ID));
    let folder_id = u64_property(value(PID_TAG_FOLDER_ID));
    let instance_key = binary_property(value(PID_TAG_INSTANCE_KEY));
    let display_name = string_property(value(PID_TAG_DISPLAY_NAME_W));
    let container_class = string_property(value(PID_TAG_CONTAINER_CLASS_W));
    let folder_type = u32_property(value(PID_TAG_FOLDER_TYPE));
    let content_count = count_property(value(PID_TAG_CONTENT_COUNT));
    let associated_content_count = count_property(value(PID_TAG_ASSOCIATED_CONTENT_COUNT));

    let entry_id_decoded = entry_id
        .as_deref()
        .and_then(crate::mapi::identity::object_id_from_folder_entry_id);
    let source_key_decoded = source_key
        .as_deref()
        .and_then(crate::mapi::identity::object_id_from_source_key);
    let parent_source_key_decoded = parent_source_key
        .as_deref()
        .and_then(crate::mapi::identity::object_id_from_source_key);
    let parent_entry_id_decoded = parent_entry_id
        .as_deref()
        .and_then(crate::mapi::identity::object_id_from_folder_entry_id);

    let folder_id_consistent = expected_folder_id
        .is_none_or(|expected| folder_id == Some(expected) && entry_id_decoded == Some(expected));
    let parent_id_consistent = expected_parent_id.is_none_or(|expected| {
        parent_source_key_decoded == Some(expected) && parent_entry_id_decoded == Some(expected)
    });
    let source_key_stable_non_empty = source_key
        .as_ref()
        .is_some_and(|bytes| !bytes.is_empty() && source_key_decoded == Some(object_id));
    let record_key_stable_non_empty = record_key
        .as_ref()
        .is_some_and(|bytes| !bytes.is_empty() && bytes == source_key.as_ref().unwrap_or(bytes));
    let instance_key_stable_non_empty =
        instance_key.as_ref().is_some_and(|bytes| !bytes.is_empty());
    let folder_property_row = !row_kind.ends_with("_associated") && row_kind != "inbox_contents";
    let folder_type_valid = !folder_property_row
        || folder_type
            .is_some_and(|value| matches!(value, FOLDER_ROOT | FOLDER_GENERIC | FOLDER_SEARCH));
    let content_count_present_non_negative = !folder_property_row || content_count.is_some();
    let associated_count_present_non_negative =
        !folder_property_row || associated_content_count.is_some();
    let container_class_status = match (expected_container_class, container_class.as_deref()) {
        (Some(expected), Some(actual)) if actual == expected => "match",
        (Some(_), Some(_)) => "mismatch",
        (Some(_), None) => "missing",
        (None, _) => "not_applicable",
    };

    let issues = [
        (!folder_id_consistent).then_some("folder_id"),
        (!parent_id_consistent).then_some("parent_id"),
        (!source_key_stable_non_empty).then_some("source_key"),
        (!record_key_stable_non_empty).then_some("record_key"),
        (!instance_key_stable_non_empty).then_some("instance_key"),
        (!folder_type_valid).then_some("folder_type"),
        (!content_count_present_non_negative).then_some("content_count"),
        (!associated_count_present_non_negative).then_some("associated_content_count"),
        (container_class_status == "mismatch" || container_class_status == "missing")
            .then_some("container_class"),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("|");

    format!(
        "row={row_index} kind={row_kind} object_id={} display_name={} entry_id={} entry_id_decoded={} record_key={} source_key={} source_key_decoded={} parent_source_key={} parent_source_key_decoded={} parent_entry_id={} parent_entry_id_decoded={} folder_id={} instance_key={} container_class={} expected_container_class={} folder_type={} content_count={} associated_content_count={} folder_id_consistent={} parent_id_consistent={} source_key_stable_non_empty={} record_key_stable_non_empty={} instance_key_stable_non_empty={} folder_type_valid={} counts_present_non_negative={} container_class_status={} issues={}",
        format_debug_u64(Some(object_id)),
        display_name.unwrap_or_default(),
        format_debug_binary(entry_id.as_deref()),
        format_debug_u64(entry_id_decoded),
        format_debug_binary(record_key.as_deref()),
        format_debug_binary(source_key.as_deref()),
        format_debug_u64(source_key_decoded),
        format_debug_binary(parent_source_key.as_deref()),
        format_debug_u64(parent_source_key_decoded),
        format_debug_binary(parent_entry_id.as_deref()),
        format_debug_u64(parent_entry_id_decoded),
        format_debug_u64(folder_id),
        format_debug_binary(instance_key.as_deref()),
        container_class.unwrap_or_default(),
        expected_container_class.unwrap_or(""),
        format_debug_u32(folder_type),
        format_debug_u32(content_count),
        format_debug_u32(associated_content_count),
        folder_id_consistent,
        parent_id_consistent,
        source_key_stable_non_empty,
        record_key_stable_non_empty,
        instance_key_stable_non_empty,
        folder_type_valid,
        content_count_present_non_negative && associated_count_present_non_negative,
        container_class_status,
        if issues.is_empty() { "none" } else { &issues },
    )
}

fn binary_property(value: Option<MapiValue>) -> Option<Vec<u8>> {
    match value {
        Some(MapiValue::Binary(bytes)) => Some(bytes),
        _ => None,
    }
}

fn string_property(value: Option<MapiValue>) -> Option<String> {
    match value {
        Some(MapiValue::String(value)) => Some(value),
        _ => None,
    }
}

fn u64_property(value: Option<MapiValue>) -> Option<u64> {
    match value {
        Some(MapiValue::U64(value)) => Some(value),
        _ => None,
    }
}

fn u32_property(value: Option<MapiValue>) -> Option<u32> {
    match value {
        Some(MapiValue::U32(value)) => Some(value),
        Some(MapiValue::I32(value)) => u32::try_from(value).ok(),
        _ => None,
    }
}

fn count_property(value: Option<MapiValue>) -> Option<u32> {
    u32_property(value)
}

pub(super) fn debug_expected_container_class(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        ROOT_FOLDER_ID
        | IPM_SUBTREE_FOLDER_ID
        | COMMON_VIEWS_FOLDER_ID
        | VIEWS_FOLDER_ID
        | SCHEDULE_FOLDER_ID
        | SEARCH_FOLDER_ID
        | SPOOLER_QUEUE_FOLDER_ID => None,
        CALENDAR_FOLDER_ID => Some("IPF.Appointment"),
        CONTACTS_FOLDER_ID | SUGGESTED_CONTACTS_FOLDER_ID | CONTACTS_SEARCH_FOLDER_ID => {
            Some("IPF.Contact")
        }
        QUICK_CONTACTS_FOLDER_ID => Some("IPF.Contact.MOC.QuickContacts"),
        IM_CONTACT_LIST_FOLDER_ID => Some("IPF.Contact.MOC.ImContactList"),
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => Some("IPF.Task"),
        NOTES_FOLDER_ID => Some("IPF.StickyNote"),
        JOURNAL_FOLDER_ID => Some("IPF.Journal"),
        SHORTCUTS_FOLDER_ID | DOCUMENT_LIBRARIES_FOLDER_ID => Some("IPF.ShortcutFolder"),
        RSS_FEEDS_FOLDER_ID => Some("IPF.Note.OutlookHomepage"),
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID | QUICK_STEP_SETTINGS_FOLDER_ID => {
            Some("IPF.Configuration")
        }
        REMINDERS_FOLDER_ID => Some("Outlook.Reminder"),
        _ => Some("IPF.Note"),
    }
}

fn format_debug_binary(value: Option<&[u8]>) -> String {
    match value {
        Some(bytes) => {
            let head = hex_lower(&bytes[..bytes.len().min(12)]);
            let tail = hex_lower(&bytes[bytes.len().saturating_sub(6)..]);
            format!("present:{}:{}..{}", bytes.len(), head, tail)
        }
        None => "missing".to_string(),
    }
}

fn format_debug_u64(value: Option<u64>) -> String {
    value
        .map(|value| format!("0x{value:016x}"))
        .unwrap_or_else(|| "missing".to_string())
}

fn format_debug_u32(value: Option<u32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "missing".to_string())
}

pub(super) fn format_table_property_tags(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",")
}
