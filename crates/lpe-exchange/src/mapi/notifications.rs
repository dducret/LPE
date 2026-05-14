use super::rop::*;

pub(in crate::mapi) const NOTIFY_CRITICAL_ERROR: u16 = 0x0001;
pub(in crate::mapi) const NOTIFY_NEW_MAIL: u16 = 0x0002;
pub(in crate::mapi) const NOTIFY_OBJECT_CREATED: u16 = 0x0004;
pub(in crate::mapi) const NOTIFY_OBJECT_DELETED: u16 = 0x0008;
pub(in crate::mapi) const NOTIFY_OBJECT_MODIFIED: u16 = 0x0010;
pub(in crate::mapi) const NOTIFY_OBJECT_MOVED: u16 = 0x0020;
pub(in crate::mapi) const NOTIFY_TABLE_MODIFIED: u16 = 0x0100;
pub(in crate::mapi) const NOTIFY_EXTENDED: u16 = 0x0400;

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
pub(in crate::mapi) struct MapiNotificationEvent {
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) kind: MapiNotificationKind,
}

pub(in crate::mapi) fn rop_register_notification_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x29, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn notification_wait_body(event_pending: bool) -> Vec<u8> {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, u32::from(event_pending));
    write_u32(&mut body, 0);
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
            registration.notification_types
                & (NOTIFY_NEW_MAIL
                    | NOTIFY_OBJECT_CREATED
                    | NOTIFY_OBJECT_DELETED
                    | NOTIFY_OBJECT_MODIFIED
                    | NOTIFY_OBJECT_MOVED
                    | NOTIFY_TABLE_MODIFIED)
                != 0
        }
        MapiNotificationKind::Hierarchy => {
            registration.notification_types
                & (NOTIFY_OBJECT_CREATED
                    | NOTIFY_OBJECT_DELETED
                    | NOTIFY_OBJECT_MODIFIED
                    | NOTIFY_OBJECT_MOVED
                    | NOTIFY_TABLE_MODIFIED)
                != 0
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
    notification_types
        & !(NOTIFY_NEW_MAIL
            | NOTIFY_CRITICAL_ERROR
            | NOTIFY_OBJECT_CREATED
            | NOTIFY_OBJECT_DELETED
            | NOTIFY_OBJECT_MODIFIED
            | NOTIFY_OBJECT_MOVED
            | NOTIFY_TABLE_MODIFIED
            | NOTIFY_EXTENDED)
        == 0
}
