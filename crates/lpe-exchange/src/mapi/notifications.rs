use super::identity::wire_id_bytes_from_object_id;
use super::rop::*;
use super::wire::{
    MapiNotificationEventMask, MAPI_CONTENT_NOTIFICATION_MASK, MAPI_HIERARCHY_NOTIFICATION_MASK,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MapiNotificationEvent {
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) message_id: Option<u64>,
    pub(in crate::mapi) old_folder_id: Option<u64>,
    pub(in crate::mapi) canonical_folder_id: Option<uuid::Uuid>,
    pub(in crate::mapi) canonical_message_id: Option<uuid::Uuid>,
    pub(in crate::mapi) kind: MapiNotificationKind,
    pub(in crate::mapi) event_mask: u16,
    pub(in crate::mapi) change_cursor: Option<i64>,
    pub(in crate::mapi) modseq: Option<u64>,
    pub(in crate::mapi) total_messages: Option<i32>,
    pub(in crate::mapi) unread_messages: Option<i32>,
    pub(in crate::mapi) object_kind: Option<&'static str>,
    pub(in crate::mapi) change_kind: Option<String>,
    pub(in crate::mapi) display_name: Option<String>,
    pub(in crate::mapi) parent_display_name: Option<String>,
    pub(in crate::mapi) message_subject: Option<String>,
}

impl MapiNotificationEvent {
    pub(in crate::mapi) fn content(folder_id: u64, message_id: Option<u64>) -> Self {
        Self {
            folder_id,
            message_id,
            old_folder_id: None,
            canonical_folder_id: None,
            canonical_message_id: None,
            kind: MapiNotificationKind::Content,
            event_mask: MapiNotificationEventMask::TableModified.as_u16(),
            change_cursor: None,
            modseq: None,
            total_messages: None,
            unread_messages: None,
            object_kind: None,
            change_kind: None,
            display_name: None,
            parent_display_name: None,
            message_subject: None,
        }
    }

    pub(in crate::mapi) fn hierarchy(folder_id: u64, changed_folder_id: Option<u64>) -> Self {
        Self {
            folder_id,
            message_id: changed_folder_id,
            old_folder_id: None,
            canonical_folder_id: None,
            canonical_message_id: None,
            kind: MapiNotificationKind::Hierarchy,
            event_mask: MapiNotificationEventMask::TableModified.as_u16(),
            change_cursor: None,
            modseq: None,
            total_messages: None,
            unread_messages: None,
            object_kind: None,
            change_kind: None,
            display_name: None,
            parent_display_name: None,
            message_subject: None,
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
        change_kind: String,
        display_name: Option<String>,
        parent_display_name: Option<String>,
        message_subject: Option<String>,
    ) -> Self {
        Self {
            folder_id,
            message_id,
            old_folder_id,
            canonical_folder_id: None,
            canonical_message_id: None,
            kind,
            event_mask,
            change_cursor: Some(change_cursor),
            modseq: Some(modseq),
            total_messages,
            unread_messages,
            object_kind: Some(match kind {
                MapiNotificationKind::Content => "mailbox_message",
                MapiNotificationKind::Hierarchy => "mailbox",
            }),
            change_kind: Some(change_kind),
            display_name,
            parent_display_name,
            message_subject,
        }
    }

    pub(crate) fn with_canonical_ids(
        mut self,
        canonical_folder_id: Option<uuid::Uuid>,
        canonical_message_id: Option<uuid::Uuid>,
    ) -> Self {
        self.canonical_folder_id = canonical_folder_id;
        self.canonical_message_id = canonical_message_id;
        self
    }

    pub(crate) fn change_cursor(&self) -> Option<i64> {
        self.change_cursor
    }

    pub(crate) fn canonical_folder_id(&self) -> Option<uuid::Uuid> {
        self.canonical_folder_id
    }

    pub(crate) fn canonical_message_id(&self) -> Option<uuid::Uuid> {
        self.canonical_message_id
    }

    pub(crate) fn change_kind(&self) -> Option<&str> {
        self.change_kind.as_deref()
    }
}

pub(in crate::mapi) fn rop_register_notification_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x29, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_notification_success_response_matches_microsoft_wire_shape() {
        let response = rop_register_notification_response(&RopRequest {
            rop_id: 0x29,
            input_handle_index: Some(0),
            output_handle_index: Some(3),
            payload: Vec::new(),
        });

        assert_eq!(response, vec![0x29, 0x03, 0, 0, 0, 0]);
    }
}

/// [MS-OXCMAPIHTTP] section 2.2.4.4.2: NotificationWait only signals that an
/// event is pending. Notification details are returned by a subsequent Execute.
pub(in crate::mapi) fn notification_wait_body(event_pending: bool) -> Vec<u8> {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, u32::from(event_pending));
    write_u32(&mut body, 0);
    body
}

/// [MS-OXCROPS] sections 2.2.14.2 and 3.1.5.1.3; [MS-OXCNOTIF]
/// section 2.2.1.4.1.2. RopNotify is an extra ROP response appended to the
/// RopsList and carries the notification subscription's server object handle.
pub(in crate::mapi) fn rop_notify_response(
    notification_handle: u32,
    logon_id: u8,
    event: &MapiNotificationEvent,
) -> Vec<u8> {
    let mut response = vec![0x2A];
    write_u32(&mut response, notification_handle);
    response.push(logon_id);
    append_notification_data(&mut response, event);
    response
}

fn append_notification_data(response: &mut Vec<u8>, event: &MapiNotificationEvent) {
    let notification_type = event.event_mask & 0x0FFF;
    let message_event = event.kind == MapiNotificationKind::Content && event.message_id.is_some();
    match notification_type {
        0x0100 => {
            write_u16(response, 0x0100);
            write_u16(response, 0x0001);
        }
        0x0010 => {
            let mut flags = 0x0010;
            if message_event {
                flags |= 0x8000;
            }
            if event.total_messages.is_some() {
                flags |= 0x1000;
            }
            if event.unread_messages.is_some() {
                flags |= 0x2000;
            }
            write_u16(response, flags);
            append_event_object_ids(response, event, message_event);
            write_u16(response, 0);
            if let Some(total_messages) = event.total_messages {
                write_u32(response, total_messages.max(0) as u32);
            }
            if let Some(unread_messages) = event.unread_messages {
                write_u32(response, unread_messages.max(0) as u32);
            }
        }
        0x0004 | 0x0008 => {
            write_u16(
                response,
                notification_type | if message_event { 0x8000 } else { 0 },
            );
            append_event_object_ids(response, event, message_event);
            if !message_event {
                append_wire_id(response, event.folder_id);
            }
            if notification_type == 0x0004 {
                write_u16(response, 0);
            }
        }
        0x0020 | 0x0040 => {
            write_u16(
                response,
                notification_type | if message_event { 0x8000 } else { 0 },
            );
            let object_id = event_object_id(event);
            append_wire_id(response, object_id);
            if message_event {
                append_wire_id(response, event.message_id.unwrap_or_default());
            } else {
                append_wire_id(response, event.folder_id);
            }
            append_wire_id(response, event.old_folder_id.unwrap_or(object_id));
            if message_event {
                append_wire_id(response, event.message_id.unwrap_or_default());
            } else {
                append_wire_id(response, event.old_folder_id.unwrap_or(event.folder_id));
            }
        }
        0x0002 if message_event => {
            write_u16(response, 0x8002);
            append_event_object_ids(response, event, true);
            write_u32(response, 0);
            response.push(0);
            response.extend_from_slice(b"IPM.Note\0");
        }
        0x0080 => {
            write_u16(response, 0x0080);
            append_wire_id(response, event_object_id(event));
        }
        _ => {
            write_u16(response, 0x0100);
            write_u16(response, 0x0001);
        }
    }
}

fn append_event_object_ids(
    response: &mut Vec<u8>,
    event: &MapiNotificationEvent,
    message_event: bool,
) {
    append_wire_id(response, event_object_id(event));
    if message_event {
        append_wire_id(response, event.message_id.unwrap_or_default());
    }
}

fn event_object_id(event: &MapiNotificationEvent) -> u64 {
    match event.kind {
        MapiNotificationKind::Content => event.folder_id,
        MapiNotificationKind::Hierarchy => event.message_id.unwrap_or(event.folder_id),
    }
}

fn append_wire_id(response: &mut Vec<u8>, object_id: u64) {
    response.extend_from_slice(&wire_id_bytes_from_object_id(object_id).unwrap_or([0; 8]));
}

pub(in crate::mapi) fn registration_matches_event(
    registration: &MapiNotificationRegistration,
    event: &MapiNotificationEvent,
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
