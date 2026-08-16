use super::*;

pub(super) fn save_attachment_parent_handle(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    attachment_handle: u32,
    folder_id: u64,
    message_id: u64,
) -> std::result::Result<u32, MapiError> {
    let Some(parent_handle) = handle_slots
        .get(usize::from(request.response_handle_index()))
        .copied()
        .filter(|handle| *handle != u32::MAX)
    else {
        return Err(MapiError::NullObject);
    };
    let Some(parent) = session.handles.get(&parent_handle) else {
        return Err(if session.issued_handles.contains(&parent_handle) {
            MapiError::InvalidObject
        } else {
            MapiError::NullObject
        });
    };

    // [MS-OXCMSG] section 2.2.3.15 requires ResponseHandleIndex to reference
    // the containing Message object. Pending parents are distinct server
    // objects, so preserve the exact parent handle captured at attachment
    // creation instead of accepting another live object with the same folder.
    if session
        .pending_attachment_parent_messages
        .get(&attachment_handle)
        .is_some_and(|expected_parent| *expected_parent != parent_handle)
    {
        return Err(MapiError::NotSupported);
    }

    let is_containing_message = match parent {
        MapiObject::Message {
            folder_id: parent_folder_id,
            message_id: parent_message_id,
            ..
        } => *parent_folder_id == folder_id && *parent_message_id == message_id,
        MapiObject::PendingMessage {
            folder_id: parent_folder_id,
            ..
        } => *parent_folder_id == folder_id && message_id == 0,
        MapiObject::Event {
            folder_id: parent_folder_id,
            event_id,
            ..
        } => *parent_folder_id == folder_id && *event_id == message_id,
        MapiObject::PendingEvent {
            folder_id: parent_folder_id,
            ..
        } => *parent_folder_id == folder_id && message_id == 0,
        MapiObject::Contact {
            folder_id: parent_folder_id,
            contact_id,
            ..
        } => *parent_folder_id == folder_id && *contact_id == message_id,
        MapiObject::PendingContact {
            folder_id: parent_folder_id,
            ..
        } => *parent_folder_id == folder_id && message_id == 0,
        _ => false,
    };
    is_containing_message
        .then_some(parent_handle)
        .ok_or(MapiError::NotSupported)
}
