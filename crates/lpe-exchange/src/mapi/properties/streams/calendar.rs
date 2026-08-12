use super::*;

pub(super) fn effective_event_stream_property_value(
    event: &crate::mapi_store::MapiEvent,
    transaction: &MapiEventTransaction,
    property_tag: u32,
    mailbox_guid: Uuid,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<MapiValue> {
    let storage_tag = canonical_calendar_property_storage_tag(property_tag);
    if transaction.deleted_properties.contains(&storage_tag) {
        return None;
    }
    if let Some(value) = transaction.pending_properties.get(&storage_tag) {
        return Some(value.clone());
    }
    if crate::mapi::dispatch::custom_properties::is_calendar_passthrough_property_tag(storage_tag) {
        return stored_event_stream_property_value(event, storage_tag);
    }
    let reminder = snapshot.reminder_for_source("calendar", event.canonical_id);
    if storage_tag == PID_TAG_SEARCH_KEY {
        versioned_event_property_value_with_reminder(event, property_tag, reminder)
    } else {
        event_property_value_with_reminder_and_mailbox_guid(
            &event.event,
            event.id,
            event.folder_id,
            property_tag,
            reminder,
            Some(mailbox_guid),
        )
    }
}

fn stored_event_stream_property_value(
    event: &crate::mapi_store::MapiEvent,
    storage_tag: u32,
) -> Option<MapiValue> {
    let stored = event
        .stored_properties
        .iter()
        .find(|value| value.property_tag == storage_tag)
        .or_else(|| {
            event.stored_properties.iter().find(|value| {
                canonical_calendar_property_storage_tag(value.property_tag) == storage_tag
            })
        })?;
    if stored.property_type != MapiPropertyTag::new(stored.property_tag).property_type_code() {
        return None;
    }
    let mut cursor = Cursor::new(&stored.property_value);
    let value = parse_mapi_property_value(&mut cursor, stored.property_tag).ok()?;
    (cursor.remaining() == 0).then_some(value)
}

pub(super) fn calendar_event_stream_property_is_writable(property_tag: u32) -> bool {
    let storage_tag = canonical_calendar_property_storage_tag(property_tag);
    !calendar_event_stream_property_id_is_server_managed(storage_tag)
        && !crate::mapi::dispatch::custom_properties::is_unsupported_calendar_passthrough_property_tag(
            storage_tag,
        )
        && (crate::mapi::dispatch::custom_properties::is_calendar_passthrough_property_tag(
            storage_tag,
        ) || matches!(
            storage_tag,
            PID_TAG_MESSAGE_CLASS_W
                | PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_BODY_W
                | PID_TAG_BODY_HTML_W
                | PID_TAG_HTML_BINARY
                | PID_TAG_SENDER_NAME_W
                | PID_TAG_SENDER_EMAIL_ADDRESS_W
                | PID_TAG_DISPLAY_TO_W
                | PID_TAG_DISPLAY_CC_W
                | PID_LID_LOCATION_W_TAG
                | PID_LID_ALL_ATTENDEES_STRING_W_TAG
                | PID_LID_TO_ATTENDEES_STRING_W_TAG
                | PID_LID_CC_ATTENDEES_STRING_W_TAG
                | PID_LID_TIME_ZONE_DESCRIPTION_W_TAG
                | PID_LID_APPOINTMENT_RECUR_TAG
        ))
}

fn calendar_event_stream_property_id_is_server_managed(property_tag: u32) -> bool {
    let property_id = property_tag & 0xFFFF_0000;
    crate::mapi::dispatch::event_transactions::event_property_is_server_managed(property_tag)
        || [
            PID_TAG_FOLDER_ID,
            PID_TAG_PARENT_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_ENTRY_ID,
            PID_TAG_PARENT_ENTRY_ID,
            PID_TAG_INSTANCE_KEY,
            PID_TAG_RECORD_KEY,
            PID_TAG_SOURCE_KEY,
            PID_TAG_PARENT_SOURCE_KEY,
            PID_TAG_SEARCH_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_CHANGE_NUMBER,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_LOCAL_COMMIT_TIME,
            PID_TAG_DISPLAY_NAME_W,
        ]
        .into_iter()
        .any(|managed_tag| managed_tag & 0xFFFF_0000 == property_id)
}

pub(super) fn insert_pending_event_stream_property(
    properties: &mut HashMap<u32, MapiValue>,
    property_tag: u32,
    value: MapiValue,
) {
    let storage_tag = canonical_calendar_property_storage_tag(property_tag);
    match (storage_tag, value) {
        (PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W, MapiValue::String(value)) => {
            properties.insert(PID_TAG_SUBJECT_W, MapiValue::String(value.clone()));
            properties.insert(PID_TAG_NORMALIZED_SUBJECT_W, MapiValue::String(value));
        }
        (PID_TAG_BODY_HTML_W, MapiValue::String(value)) => {
            properties.insert(
                PID_TAG_HTML_BINARY,
                MapiValue::Binary(value.as_bytes().to_vec()),
            );
            properties.insert(PID_TAG_BODY_HTML_W, MapiValue::String(value));
        }
        (PID_TAG_HTML_BINARY, MapiValue::Binary(value)) => {
            properties.remove(&PID_TAG_BODY_HTML_W);
            if let Ok(html) = String::from_utf8(value.clone()) {
                properties.insert(PID_TAG_BODY_HTML_W, MapiValue::String(html));
            }
            properties.insert(PID_TAG_HTML_BINARY, MapiValue::Binary(value));
        }
        (_, value) => {
            properties.insert(storage_tag, value);
        }
    }
}

pub(super) fn insert_event_stream_property(
    transaction: &mut MapiEventTransaction,
    property_tag: u32,
    value: MapiValue,
) {
    let storage_tag = canonical_calendar_property_storage_tag(property_tag);
    transaction.deleted_properties.remove(&storage_tag);
    match (storage_tag, value) {
        (PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W, MapiValue::String(value)) => {
            transaction.deleted_properties.remove(&PID_TAG_SUBJECT_W);
            transaction
                .deleted_properties
                .remove(&PID_TAG_NORMALIZED_SUBJECT_W);
            transaction
                .pending_properties
                .insert(PID_TAG_SUBJECT_W, MapiValue::String(value.clone()));
            transaction
                .pending_properties
                .insert(PID_TAG_NORMALIZED_SUBJECT_W, MapiValue::String(value));
        }
        (PID_TAG_BODY_HTML_W, MapiValue::String(value)) => {
            transaction.deleted_properties.remove(&PID_TAG_HTML_BINARY);
            transaction.pending_properties.insert(
                PID_TAG_HTML_BINARY,
                MapiValue::Binary(value.as_bytes().to_vec()),
            );
            transaction
                .pending_properties
                .insert(PID_TAG_BODY_HTML_W, MapiValue::String(value));
        }
        (PID_TAG_HTML_BINARY, MapiValue::Binary(value)) => {
            transaction.deleted_properties.remove(&PID_TAG_BODY_HTML_W);
            transaction.pending_properties.remove(&PID_TAG_BODY_HTML_W);
            if let Ok(html) = String::from_utf8(value.clone()) {
                transaction
                    .pending_properties
                    .insert(PID_TAG_BODY_HTML_W, MapiValue::String(html));
            }
            transaction
                .pending_properties
                .insert(PID_TAG_HTML_BINARY, MapiValue::Binary(value));
        }
        (_, value) => {
            transaction.pending_properties.insert(storage_tag, value);
        }
    }
}
