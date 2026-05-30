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
pub(crate) enum MapiNotificationKind {
    Content,
    Hierarchy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MapiNotificationEvent {
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) message_id: Option<u64>,
    pub(in crate::mapi) old_folder_id: Option<u64>,
    pub(in crate::mapi) kind: MapiNotificationKind,
    pub(in crate::mapi) event_mask: u16,
    pub(in crate::mapi) change_cursor: Option<i64>,
    pub(in crate::mapi) modseq: Option<u64>,
    pub(in crate::mapi) total_messages: Option<i32>,
    pub(in crate::mapi) unread_messages: Option<i32>,
}

impl MapiNotificationEvent {
    pub(in crate::mapi) fn content(folder_id: u64, message_id: Option<u64>) -> Self {
        Self {
            folder_id,
            message_id,
            old_folder_id: None,
            kind: MapiNotificationKind::Content,
            event_mask: MapiNotificationEventMask::TableModified.as_u16(),
            change_cursor: None,
            modseq: None,
            total_messages: None,
            unread_messages: None,
        }
    }

    pub(in crate::mapi) fn hierarchy(folder_id: u64, changed_folder_id: Option<u64>) -> Self {
        Self {
            folder_id,
            message_id: changed_folder_id,
            old_folder_id: None,
            kind: MapiNotificationKind::Hierarchy,
            event_mask: MapiNotificationEventMask::TableModified.as_u16(),
            change_cursor: None,
            modseq: None,
            total_messages: None,
            unread_messages: None,
        }
    }

    pub(crate) fn canonical(
        kind: MapiNotificationKind,
        event_mask: u16,
        folder_id: u64,
        message_id: Option<u64>,
        old_folder_id: Option<u64>,
        change_cursor: i64,
        modseq: u64,
        total_messages: Option<i32>,
        unread_messages: Option<i32>,
    ) -> Self {
        Self {
            folder_id,
            message_id,
            old_folder_id,
            kind,
            event_mask,
            change_cursor: Some(change_cursor),
            modseq: Some(modseq),
            total_messages,
            unread_messages,
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
        body.push(
            u8::from(event.message_id.is_some())
                | (u8::from(event.old_folder_id.is_some()) << 1)
                | (u8::from(event.total_messages.is_some() || event.unread_messages.is_some())
                    << 2),
        );
        body.extend_from_slice(&wire_id_bytes_from_object_id(event.folder_id).unwrap_or([0; 8]));
        body.extend_from_slice(
            &event
                .message_id
                .and_then(wire_id_bytes_from_object_id)
                .unwrap_or([0; 8]),
        );
        body.extend_from_slice(
            &event
                .old_folder_id
                .and_then(wire_id_bytes_from_object_id)
                .unwrap_or([0; 8]),
        );
        write_u64(&mut body, event.change_cursor.unwrap_or_default() as u64);
        write_u64(&mut body, event.modseq.unwrap_or_default());
        write_u32(&mut body, event.total_messages.unwrap_or(-1) as u32);
        write_u32(&mut body, event.unread_messages.unwrap_or(-1) as u32);
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
            notification_type_matches(registration.notification_types, event.event_mask)
                && registration.notification_types & MAPI_CONTENT_NOTIFICATION_MASK != 0
        }
        MapiNotificationKind::Hierarchy => {
            notification_type_matches(registration.notification_types, event.event_mask)
                && registration.notification_types & MAPI_HIERARCHY_NOTIFICATION_MASK != 0
        }
    }
}

fn notification_type_matches(requested: u16, event_mask: u16) -> bool {
    requested & event_mask != 0
        || requested & MapiNotificationEventMask::TableModified.as_u16() != 0
        || event_mask == MapiNotificationEventMask::TableModified.as_u16()
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
