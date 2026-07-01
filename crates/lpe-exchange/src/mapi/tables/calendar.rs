use super::*;

pub(in crate::mapi) fn calendar_content_rows<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    folder_id: u64,
    restriction: Option<&MapiRestriction>,
) -> Vec<&'a crate::mapi_store::MapiEvent> {
    let mut rows = snapshot.events_for_folder(folder_id);
    rows.retain(|event| restriction_matches_event(restriction, event));
    rows
}

pub(super) fn restriction_matches_event(
    restriction: Option<&MapiRestriction>,
    event: &crate::mapi_store::MapiEvent,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        event_property_value(&event.event, event.id, event.folder_id, property_tag)
    })
}
