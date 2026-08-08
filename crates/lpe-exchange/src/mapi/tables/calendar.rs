use super::*;

pub(in crate::mapi) fn calendar_content_rows<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    folder_id: u64,
    restriction: Option<&MapiRestriction>,
) -> Vec<&'a crate::mapi_store::MapiEvent> {
    calendar_content_rows_with_mailbox_guid(snapshot, folder_id, restriction, Uuid::nil())
}

pub(in crate::mapi) fn calendar_content_rows_with_mailbox_guid<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    folder_id: u64,
    restriction: Option<&MapiRestriction>,
    mailbox_guid: Uuid,
) -> Vec<&'a crate::mapi_store::MapiEvent> {
    let mut rows = snapshot.events_for_folder(folder_id);
    rows.retain(|event| {
        restriction_matches_event_with_mailbox_guid(restriction, event, mailbox_guid)
    });
    rows
}

pub(super) fn restriction_matches_event_with_mailbox_guid(
    restriction: Option<&MapiRestriction>,
    event: &crate::mapi_store::MapiEvent,
    mailbox_guid: Uuid,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        match canonical_property_storage_tag(property_tag) {
            PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
                mailbox_guid,
                event.folder_id,
                event.id,
            )
            .map(MapiValue::Binary),
            PID_TAG_PARENT_ENTRY_ID => {
                crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, event.folder_id)
                    .map(MapiValue::Binary)
            }
            PID_TAG_RECORD_KEY => Some(MapiValue::Binary(event.source_key.clone())),
            _ => versioned_event_property_value_with_reminder(event, property_tag, None),
        }
    })
}
