use super::identity::wire_id_bytes_from_object_id;
use super::rop::*;
use super::wire::{
    MapiNotificationEventMask, MAPI_CONTENT_NOTIFICATION_MASK, MAPI_HIERARCHY_NOTIFICATION_MASK,
    MAPI_SUPPORTED_NOTIFICATION_MASK,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct MapiNotificationRegistration {
    pub(in crate::mapi) notification_types: u16,
    pub(in crate::mapi) folder_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum MapiNotificationKind {
    Content,
    Hierarchy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MapiNotificationEvent {
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) message_id: Option<u64>,
    pub(in crate::mapi) kind: MapiNotificationKind,
    pub(in crate::mapi) event_mask: u16,
}

impl MapiNotificationEvent {
    pub(in crate::mapi) fn content(folder_id: u64, message_id: Option<u64>) -> Self {
        Self {
            folder_id,
            message_id,
            kind: MapiNotificationKind::Content,
            event_mask: MapiNotificationEventMask::TableModified.as_u16(),
        }
    }

    pub(in crate::mapi) fn hierarchy(folder_id: u64, changed_folder_id: Option<u64>) -> Self {
        Self {
            folder_id,
            message_id: changed_folder_id,
            kind: MapiNotificationKind::Hierarchy,
            event_mask: MapiNotificationEventMask::TableModified.as_u16(),
        }
    }
}

pub(in crate::mapi) fn rop_register_notification_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x29, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn notification_wait_body_with_events(
    event_pending: bool,
    events: &[MapiNotificationEvent],
) -> Vec<u8> {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, u32::from(event_pending));
    write_u32(&mut body, events.len().min(u32::MAX as usize) as u32);
    for event in events.iter().take(u32::MAX as usize) {
        write_u16(&mut body, event.event_mask);
        body.push(match event.kind {
            MapiNotificationKind::Content => 1,
            MapiNotificationKind::Hierarchy => 2,
        });
        body.push(u8::from(event.message_id.is_some()));
        body.extend_from_slice(&wire_id_bytes_from_object_id(event.folder_id).unwrap_or([0; 8]));
        body.extend_from_slice(
            &event
                .message_id
                .and_then(wire_id_bytes_from_object_id)
                .unwrap_or([0; 8]),
        );
    }
    body
}

pub(in crate::mapi) fn registration_matches_event(
    registration: &MapiNotificationRegistration,
    event: MapiNotificationEvent,
) -> bool {
    if let Some(folder_id) = registration.folder_id {
        if folder_id != event.folder_id {
            return false;
        }
    }

    match event.kind {
        MapiNotificationKind::Content => {
            registration.notification_types & MAPI_CONTENT_NOTIFICATION_MASK != 0
        }
        MapiNotificationKind::Hierarchy => {
            registration.notification_types & MAPI_HIERARCHY_NOTIFICATION_MASK != 0
        }
    }
}

pub(in crate::mapi) fn notification_registration_from_request(
    request: &RopRequest,
) -> MapiNotificationRegistration {
    let notification_types = request.notification_types().unwrap_or(0);
    let folder_id = if request.notification_want_whole_store().unwrap_or(true) {
        None
    } else {
        request.notification_folder_id()
    };
    MapiNotificationRegistration {
        notification_types,
        folder_id,
    }
}

pub(in crate::mapi) fn supported_notification_types(notification_types: u16) -> bool {
    notification_types & !MAPI_SUPPORTED_NOTIFICATION_MASK == 0
}
