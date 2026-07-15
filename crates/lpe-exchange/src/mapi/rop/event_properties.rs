use super::*;

pub(in crate::mapi) fn event_object_property_is_deleted(
    object: Option<&MapiObject>,
    property_tag: u32,
) -> bool {
    matches!(
        object,
        Some(MapiObject::Event { transaction, .. })
            if transaction
                .deleted_properties
                .contains(&canonical_property_storage_tag(property_tag))
    )
}

pub(in crate::mapi) fn serialize_event_object_property(
    object: &MapiObject,
    snapshot: &MapiMailStoreSnapshot,
    property_tag: u32,
) -> Vec<u8> {
    let MapiObject::Event {
        folder_id,
        event_id,
        transaction,
    } = object
    else {
        unreachable!("event property serializer requires an Event object")
    };
    snapshot
        .event_for_id(*folder_id, *event_id)
        .map(|event| {
            let storage_tag = canonical_property_storage_tag(property_tag);
            if transaction.deleted_properties.contains(&storage_tag) {
                let mut value = Vec::new();
                write_property_default(&mut value, property_tag);
                return value;
            }
            if let Some(value) = transaction.pending_properties.get(&storage_tag) {
                let mut serialized = Vec::new();
                write_mapi_value(&mut serialized, property_tag, value);
                return serialized;
            }
            serialize_versioned_event_row_with_reminder_and_attachments(
                event,
                snapshot.reminder_for_source("calendar", event.canonical_id),
                !event.attachments.is_empty(),
                &[property_tag],
            )
        })
        .unwrap_or_else(|| {
            let mut value = Vec::new();
            write_property_default(&mut value, property_tag);
            value
        })
}
