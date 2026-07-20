use super::*;

pub(in crate::mapi) fn contact_size(contact: &AccessibleContact) -> i64 {
    contact
        .name
        .len()
        .saturating_add(contact.email.len())
        .saturating_add(contact.phone.len())
        .saturating_add(contact.team.len())
        .saturating_add(contact.notes.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn event_size(event: &AccessibleEvent) -> i64 {
    event
        .title
        .len()
        .saturating_add(event.location.len())
        .saturating_add(event.notes.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn task_size(task: &ClientTask) -> i64 {
    task.title
        .len()
        .saturating_add(task.description.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn journal_entry_start_sort_key(entry: &JournalEntry) -> &str {
    entry
        .starts_at
        .as_deref()
        .or(entry.occurred_at.as_deref())
        .unwrap_or(&entry.updated_at)
}

pub(in crate::mapi) fn serialize_contact_row(
    contact: &AccessibleContact,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match contact_table_property_value(contact, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_mapi_contact_row(
    contact: &MapiContact,
    folder_id: u64,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match contact_property_value_with_identity(
            &contact.contact,
            contact.id,
            folder_id,
            mailbox_guid,
            contact.durable_identity.as_ref(),
            *column,
        )
        .or_else(|| {
            outlook_contact_empty_email_table_value(canonical_property_storage_tag(*column))
        }) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(super) fn contact_table_property_value(
    contact: &AccessibleContact,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    contact_property_value(contact, item_id, folder_id, property_tag).or_else(|| {
        outlook_contact_empty_email_table_value(canonical_property_storage_tag(property_tag))
    })
}

fn outlook_contact_empty_email_table_value(property_tag: u32) -> Option<MapiValue> {
    match property_tag {
        PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG
        | PID_LID_EMAIL2_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG
        | PID_LID_EMAIL3_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME_W_TAG => Some(MapiValue::String(String::new())),
        _ => None,
    }
}

pub(in crate::mapi) fn serialize_event_row(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    serialize_event_row_with_attachments(event, item_id, folder_id, false, columns)
}

pub(in crate::mapi) fn serialize_event_row_with_attachments(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    has_attachments: bool,
    columns: &[u32],
) -> Vec<u8> {
    serialize_event_row_with_reminder_and_attachments(
        event,
        item_id,
        folder_id,
        None,
        has_attachments,
        columns,
    )
}

pub(in crate::mapi) fn serialize_event_row_with_reminder_and_attachments(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    reminder: Option<&lpe_storage::ClientReminder>,
    has_attachments: bool,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match if canonical_property_storage_tag(*column) == PID_TAG_HAS_ATTACHMENTS {
            Some(MapiValue::Bool(has_attachments))
        } else {
            event_property_value_with_reminder(event, item_id, folder_id, *column, reminder)
        } {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_versioned_event_row_with_reminder_and_attachments(
    event: &crate::mapi_store::MapiEvent,
    reminder: Option<&lpe_storage::ClientReminder>,
    has_attachments: bool,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match if canonical_property_storage_tag(*column) == PID_TAG_HAS_ATTACHMENTS {
            Some(MapiValue::Bool(has_attachments))
        } else {
            versioned_event_property_value_with_reminder(event, *column, reminder)
        } {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_versioned_event_row(
    event: &crate::mapi_store::MapiEvent,
    columns: &[u32],
) -> Vec<u8> {
    serialize_versioned_event_row_with_reminder_and_attachments(
        event,
        None,
        !event.attachments.is_empty(),
        columns,
    )
}

pub(in crate::mapi) fn serialize_task_row(
    task: &ClientTask,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match task_property_value(task, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_reminder_task_row(
    task: &crate::mapi_store::MapiTask,
    reminder: Option<&lpe_storage::ClientReminder>,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match task_property_value_with_reminder(
            &task.task,
            task.id,
            REMINDERS_FOLDER_ID,
            *column,
            reminder,
        ) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_note_row(
    note: &ClientNote,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match note_property_value(note, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_journal_entry_row(
    entry: &JournalEntry,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match journal_entry_property_value(entry, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}
