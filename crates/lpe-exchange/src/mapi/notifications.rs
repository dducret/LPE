#[cfg(test)]
use super::identity::wire_id_bytes_from_object_id;
use super::rop::*;
use super::wire::{
    MapiNotificationEventMask, MAPI_CONTENT_NOTIFICATION_MASK, MAPI_HIERARCHY_NOTIFICATION_MASK,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct MapiNotificationRegistration {
    // [MS-OXCROPS] section 2.2.14.2: RopNotify carries the associated LogonId.
    pub(in crate::mapi) logon_id: u8,
    pub(in crate::mapi) notification_types: u16,
    pub(in crate::mapi) folder_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MapiNotificationKind {
    Content,
    Hierarchy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MapiNotificationEvent {
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) parent_folder_id: Option<u64>,
    pub(in crate::mapi) message_id: Option<u64>,
    pub(in crate::mapi) old_folder_id: Option<u64>,
    pub(in crate::mapi) old_parent_folder_id: Option<u64>,
    pub(in crate::mapi) old_message_id: Option<u64>,
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
    pub(in crate::mapi) message_class: Option<String>,
}

impl MapiNotificationEvent {
    pub(in crate::mapi) fn content(folder_id: u64, message_id: Option<u64>) -> Self {
        Self {
            folder_id,
            parent_folder_id: None,
            message_id,
            old_folder_id: None,
            old_parent_folder_id: None,
            old_message_id: None,
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
            message_class: None,
        }
    }

    pub(in crate::mapi) fn hierarchy(folder_id: u64, changed_folder_id: Option<u64>) -> Self {
        Self {
            folder_id,
            parent_folder_id: None,
            message_id: changed_folder_id,
            old_folder_id: None,
            old_parent_folder_id: None,
            old_message_id: None,
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
            message_class: None,
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
        message_class: Option<String>,
    ) -> Self {
        Self {
            folder_id,
            parent_folder_id: None,
            message_id,
            old_folder_id,
            old_parent_folder_id: None,
            old_message_id: None,
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
            message_class,
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

    pub(crate) fn with_parent_folder_id(mut self, parent_folder_id: Option<u64>) -> Self {
        self.parent_folder_id = parent_folder_id;
        self
    }

    pub(crate) fn with_old_message_id(mut self, old_message_id: Option<u64>) -> Self {
        self.old_message_id = old_message_id;
        self
    }

    pub(crate) fn with_old_parent_folder_id(mut self, old_parent_folder_id: Option<u64>) -> Self {
        self.old_parent_folder_id = old_parent_folder_id;
        self
    }

    /// [MS-OXCNOTIF] section 2.2.1.4.1.2 gives hierarchy moves and copies
    /// separate destination FolderId/ParentFolderId and source
    /// OldFolderId/OldParentFolderId fields.
    pub(crate) fn hierarchy_move_or_copy(
        event_mask: u16,
        parent_folder_id: u64,
        folder_id: u64,
        old_folder_id: u64,
        old_parent_folder_id: u64,
    ) -> Self {
        debug_assert!(matches!(event_mask, 0x0020 | 0x0040));
        Self {
            folder_id: parent_folder_id,
            parent_folder_id: None,
            message_id: Some(folder_id),
            old_folder_id: Some(old_folder_id),
            old_parent_folder_id: Some(old_parent_folder_id),
            old_message_id: None,
            canonical_folder_id: None,
            canonical_message_id: None,
            kind: MapiNotificationKind::Hierarchy,
            event_mask,
            change_cursor: None,
            modseq: None,
            total_messages: None,
            unread_messages: None,
            object_kind: Some("mailbox"),
            change_kind: Some(
                if event_mask == MapiNotificationEventMask::ObjectMoved.as_u16() {
                    "moved"
                } else {
                    "copied"
                }
                .to_string(),
            ),
            display_name: None,
            parent_display_name: None,
            message_subject: None,
            message_class: None,
        }
    }

    pub(crate) fn with_object_kind(mut self, object_kind: &'static str) -> Self {
        self.object_kind = Some(object_kind);
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

    /// [MS-OXCNOTIF] section 2.2.1.4.1.2 gives a message move/copy its
    /// destination and source FolderId/MessageId pair. The encoder must not
    /// infer source values from destination values.
    pub(crate) fn is_complete_for_wire(&self) -> bool {
        if !matches!(self.event_mask & 0x0FFF, 0x0020 | 0x0040) {
            return true;
        }
        self.old_folder_id.is_some()
            && match self.kind {
                MapiNotificationKind::Content => {
                    self.message_id.is_some() && self.old_message_id.is_some()
                }
                MapiNotificationKind::Hierarchy => {
                    self.message_id.is_some() && self.old_parent_folder_id.is_some()
                }
            }
    }

    /// A folder move changes both parents' hierarchy tables. The primary
    /// ObjectMoved event refreshes the destination table; this companion
    /// TableModified event refreshes the source table.
    pub(crate) fn source_hierarchy_table_event(&self) -> Option<Self> {
        if self.kind != MapiNotificationKind::Hierarchy
            || self.event_mask & 0x0FFF != MapiNotificationEventMask::ObjectMoved.as_u16()
        {
            return None;
        }
        Some(Self::hierarchy(self.old_parent_folder_id?, self.message_id))
    }

    #[cfg(test)]
    pub(crate) fn old_parent_folder_id(&self) -> Option<u64> {
        self.old_parent_folder_id
    }

    #[cfg(test)]
    pub(crate) fn parent_folder_id(&self) -> Option<u64> {
        self.parent_folder_id
    }

    #[cfg(test)]
    pub(crate) fn notification_test_shape(
        &self,
    ) -> (
        MapiNotificationKind,
        u16,
        u64,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<&'static str>,
    ) {
        (
            self.kind,
            self.event_mask,
            self.folder_id,
            self.message_id,
            self.old_folder_id,
            self.old_message_id,
            self.object_kind,
        )
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

    #[test]
    fn new_mail_notification_with_message_id_encodes_exchange_zero_message_flags() {
        // [MS-OXCNOTIF] section 4 shows NewMail with both FolderId and MessageId.
        let identity_codec = crate::mapi::identity::MapiIdentityCodec::legacy_for_tests();
        let folder_id = 0x0000_0000_0005_0001;
        let message_id = 0x0000_0001_009a_0001;
        let event = MapiNotificationEvent::canonical(
            MapiNotificationKind::Content,
            MapiNotificationEventMask::NewMail.as_u16(),
            folder_id,
            Some(message_id),
            None,
            1,
            1,
            None,
            None,
            "created".to_string(),
            None,
            None,
            None,
            Some("IPM.Appointment".to_string()),
        );

        let response = rop_notify_response(&identity_codec, 3, 0, &event)
            .expect("complete NewMail notification serializes");

        assert_eq!(response[0], 0x2A);
        assert_eq!(&response[1..5], &3u32.to_le_bytes());
        assert_eq!(&response[6..8], &0x8002u16.to_le_bytes());
        assert_eq!(
            &response[8..16],
            &wire_id_bytes_from_object_id(folder_id).unwrap()
        );
        assert_eq!(
            &response[16..24],
            &wire_id_bytes_from_object_id(message_id).unwrap()
        );
        // [MS-OXCNOTIF] section 2.2.1.4.1.2, implementation note <10>:
        // Exchange 2016 test1_202608031300.saz raw/753 uses zero here.
        assert_eq!(&response[24..28], &0u32.to_le_bytes());
        assert_eq!(&response[28..], b"\0IPM.Appointment\0");
        assert_ne!(&response[6..8], &0x0100u16.to_le_bytes());
    }

    #[test]
    fn new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags() {
        let identity_codec = crate::mapi::identity::MapiIdentityCodec::legacy_for_tests();
        let event = MapiNotificationEvent::canonical(
            MapiNotificationKind::Content,
            MapiNotificationEventMask::NewMail.as_u16(),
            0x0000_0000_0005_0001,
            Some(0x0000_0001_009a_0001),
            None,
            1,
            1,
            None,
            None,
            "created".to_string(),
            None,
            None,
            None,
            None,
        );

        let response = rop_notify_response(&identity_codec, 3, 0, &event)
            .expect("complete NewMail notification serializes");

        // [MS-OXCNOTIF] section 2.2.1.4.1.2, implementation note <10>:
        // Exchange 2016 test1_202608031300.saz raw/753 also uses zero here.
        assert_eq!(&response[24..28], &0u32.to_le_bytes());
        assert_eq!(&response[28..], b"\0IPM.Note\0");
    }

    #[test]
    fn object_moved_and_copied_notifications_preserve_source_message_id() {
        let identity_codec = crate::mapi::identity::MapiIdentityCodec::legacy_for_tests();
        let folder_id = 0x0000_0000_0006_0001;
        let message_id = 0x0000_0001_009a_0001;
        let old_folder_id = 0x0000_0000_0005_0001;
        let old_message_id = 0x0000_0001_0089_0001;

        for (event_mask, expected_flags) in [
            (MapiNotificationEventMask::ObjectMoved.as_u16(), 0x8020u16),
            (MapiNotificationEventMask::ObjectCopied.as_u16(), 0x8040u16),
        ] {
            let event = MapiNotificationEvent::canonical(
                MapiNotificationKind::Content,
                event_mask,
                folder_id,
                Some(message_id),
                Some(old_folder_id),
                1,
                1,
                None,
                None,
                "moved".to_string(),
                None,
                None,
                None,
                None,
            )
            .with_old_message_id(Some(old_message_id));

            let response = rop_notify_response(&identity_codec, 3, 0, &event)
                .expect("complete movement notification serializes");

            assert_eq!(&response[6..8], &expected_flags.to_le_bytes());
            assert_eq!(
                &response[8..16],
                &wire_id_bytes_from_object_id(folder_id).unwrap()
            );
            assert_eq!(
                &response[16..24],
                &wire_id_bytes_from_object_id(message_id).unwrap()
            );
            assert_eq!(
                &response[24..32],
                &wire_id_bytes_from_object_id(old_folder_id).unwrap()
            );
            assert_eq!(
                &response[32..40],
                &wire_id_bytes_from_object_id(old_message_id).unwrap()
            );
        }
    }

    #[test]
    fn hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately() {
        let identity_codec = crate::mapi::identity::MapiIdentityCodec::legacy_for_tests();
        let destination_parent_folder_id = 0x0000_0000_0006_0001;
        let destination_folder_id = 0x0000_0000_0007_0001;
        let source_folder_id = 0x0000_0000_0005_0001;
        let source_parent_folder_id = 0x0000_0000_0004_0001;

        for (event_mask, old_folder_id, expected_flags) in [
            (
                MapiNotificationEventMask::ObjectMoved.as_u16(),
                destination_folder_id,
                0x0020u16,
            ),
            (
                MapiNotificationEventMask::ObjectCopied.as_u16(),
                source_folder_id,
                0x0040u16,
            ),
        ] {
            let event = MapiNotificationEvent::hierarchy_move_or_copy(
                event_mask,
                destination_parent_folder_id,
                destination_folder_id,
                old_folder_id,
                source_parent_folder_id,
            );

            let response = rop_notify_response(&identity_codec, 3, 0, &event)
                .expect("complete hierarchy movement notification serializes");

            let mut expected = vec![0x2A, 0x03, 0, 0, 0, 0];
            expected.extend_from_slice(&expected_flags.to_le_bytes());
            expected.extend_from_slice(&[0x01, 0, 0, 0, 0, 0, 0, 0x07]);
            expected.extend_from_slice(&[0x01, 0, 0, 0, 0, 0, 0, 0x06]);
            expected.extend_from_slice(&[
                0x01,
                0,
                0,
                0,
                0,
                0,
                0,
                if event_mask == MapiNotificationEventMask::ObjectMoved.as_u16() {
                    0x07
                } else {
                    0x05
                },
            ]);
            expected.extend_from_slice(&[0x01, 0, 0, 0, 0, 0, 0, 0x04]);
            assert_eq!(response, expected);
        }
    }

    #[test]
    fn hierarchy_move_emits_a_source_parent_table_refresh() {
        let destination_parent_folder_id = 0x0000_0000_0006_0001;
        let destination_folder_id = 0x0000_0000_0007_0001;
        let source_parent_folder_id = 0x0000_0000_0004_0001;
        let moved = MapiNotificationEvent::hierarchy_move_or_copy(
            MapiNotificationEventMask::ObjectMoved.as_u16(),
            destination_parent_folder_id,
            destination_folder_id,
            destination_folder_id,
            source_parent_folder_id,
        );

        assert_eq!(
            moved
                .source_hierarchy_table_event()
                .expect("folder move source table refresh")
                .notification_test_shape(),
            (
                MapiNotificationKind::Hierarchy,
                MapiNotificationEventMask::TableModified.as_u16(),
                source_parent_folder_id,
                Some(destination_folder_id),
                None,
                None,
                None,
            )
        );
        let copied = MapiNotificationEvent::hierarchy_move_or_copy(
            MapiNotificationEventMask::ObjectCopied.as_u16(),
            destination_parent_folder_id,
            destination_folder_id,
            0x0000_0000_0005_0001,
            source_parent_folder_id,
        );
        assert!(copied.source_hierarchy_table_event().is_none());
    }

    #[test]
    fn incomplete_message_move_notifications_are_not_serialized() {
        let identity_codec = crate::mapi::identity::MapiIdentityCodec::legacy_for_tests();
        let event = MapiNotificationEvent::canonical(
            MapiNotificationKind::Content,
            MapiNotificationEventMask::ObjectMoved.as_u16(),
            0x0000_0000_0006_0001,
            Some(0x0000_0001_009a_0001),
            Some(0x0000_0000_0005_0001),
            1,
            1,
            None,
            None,
            "moved".to_string(),
            None,
            None,
            None,
            None,
        );

        assert!(rop_notify_response(&identity_codec, 3, 0, &event).is_none());

        let event_without_old_folder = MapiNotificationEvent::canonical(
            MapiNotificationKind::Content,
            MapiNotificationEventMask::ObjectMoved.as_u16(),
            0x0000_0000_0006_0001,
            Some(0x0000_0001_009a_0001),
            None,
            1,
            1,
            None,
            None,
            "moved".to_string(),
            None,
            None,
            None,
            None,
        )
        .with_old_message_id(Some(0x0000_0001_0089_0001));

        assert!(rop_notify_response(&identity_codec, 3, 0, &event_without_old_folder).is_none());
    }

    #[test]
    fn incomplete_hierarchy_move_notification_is_not_serialized() {
        let identity_codec = crate::mapi::identity::MapiIdentityCodec::legacy_for_tests();
        let event = MapiNotificationEvent::canonical(
            MapiNotificationKind::Hierarchy,
            MapiNotificationEventMask::ObjectMoved.as_u16(),
            0x0000_0000_0006_0001,
            Some(0x0000_0000_0007_0001),
            Some(0x0000_0000_0005_0001),
            1,
            1,
            None,
            None,
            "moved".to_string(),
            None,
            None,
            None,
            None,
        );

        assert!(rop_notify_response(&identity_codec, 3, 0, &event).is_none());
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
    identity_codec: &crate::mapi::identity::MapiIdentityCodec,
    notification_handle: u32,
    logon_id: u8,
    event: &MapiNotificationEvent,
) -> Option<Vec<u8>> {
    if !event.is_complete_for_wire() {
        return None;
    }
    let mut response = vec![0x2A];
    write_u32(&mut response, notification_handle);
    response.push(logon_id);
    append_notification_data(&mut response, identity_codec, event);
    Some(response)
}

fn append_notification_data(
    response: &mut Vec<u8>,
    identity_codec: &crate::mapi::identity::MapiIdentityCodec,
    event: &MapiNotificationEvent,
) {
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
            append_event_object_ids(response, identity_codec, event, message_event);
            // [MS-OXCNOTIF] section 2.2.1.4.1.2: ObjectModified TagCount
            // SHOULD be zero. The nonzero vectors in the examples are legacy
            // Exchange behavior and are not the Exchange 2016 reference shape.
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
            append_event_object_ids(response, identity_codec, event, message_event);
            if !message_event {
                append_wire_id(response, identity_codec, event.folder_id);
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
            append_wire_id(response, identity_codec, object_id);
            if message_event {
                append_wire_id(
                    response,
                    identity_codec,
                    event.message_id.unwrap_or_default(),
                );
            } else {
                append_wire_id(response, identity_codec, event.folder_id);
            }
            append_wire_id(
                response,
                identity_codec,
                event
                    .old_folder_id
                    .expect("movement notification was validated before encoding"),
            );
            if message_event {
                append_wire_id(
                    response,
                    identity_codec,
                    event
                        .old_message_id
                        .expect("movement notification was validated before encoding"),
                );
            } else {
                append_wire_id(
                    response,
                    identity_codec,
                    event
                        .old_parent_folder_id
                        .expect("movement notification was validated before encoding"),
                );
            }
        }
        0x0002 if message_event => {
            write_u16(response, 0x8002);
            append_event_object_ids(response, identity_codec, event, true);
            // [MS-OXCNOTIF] section 2.2.1.4.1.2 defines this NewMail field.
            // Its implementation note <10> and Exchange 2016
            // test1_202608031300.saz raw/753 use zero, independently of the
            // canonical PidTagMessageFlags projection ([MS-OXCMSG] section 2.2.1.6).
            write_u32(response, 0);
            // [MS-OXCNOTIF] section 2.2.1.4.1.2 carries the MessageClass
            // after UnicodeFlag. The compatibility default matches the
            // canonical class of ordinary LPE Inbox mail.
            response.push(0);
            response.extend_from_slice(
                event
                    .message_class
                    .as_deref()
                    .unwrap_or("IPM.Note")
                    .as_bytes(),
            );
            response.push(0);
        }
        0x0080 => {
            write_u16(response, 0x0080);
            append_wire_id(response, identity_codec, event_object_id(event));
        }
        _ => {
            write_u16(response, 0x0100);
            write_u16(response, 0x0001);
        }
    }
}

fn append_event_object_ids(
    response: &mut Vec<u8>,
    identity_codec: &crate::mapi::identity::MapiIdentityCodec,
    event: &MapiNotificationEvent,
    message_event: bool,
) {
    append_wire_id(response, identity_codec, event_object_id(event));
    if message_event {
        append_wire_id(
            response,
            identity_codec,
            event.message_id.unwrap_or_default(),
        );
    }
}

fn event_object_id(event: &MapiNotificationEvent) -> u64 {
    match event.kind {
        MapiNotificationKind::Content => event.folder_id,
        MapiNotificationKind::Hierarchy => event.message_id.unwrap_or(event.folder_id),
    }
}

fn append_wire_id(
    response: &mut Vec<u8>,
    identity_codec: &crate::mapi::identity::MapiIdentityCodec,
    object_id: u64,
) {
    response.extend_from_slice(
        &identity_codec
            .wire_id_bytes_from_object_id(object_id)
            .unwrap_or([0; 8]),
    );
}

pub(in crate::mapi) fn registration_matches_event(
    registration: &MapiNotificationRegistration,
    event: &MapiNotificationEvent,
) -> bool {
    if let Some(folder_id) = registration.folder_id {
        let hierarchy_movement_from_registered_parent = event.kind
            == MapiNotificationKind::Hierarchy
            && matches!(event.event_mask & 0x0FFF, 0x0020 | 0x0040)
            && event.old_parent_folder_id == Some(folder_id);
        if folder_id != event.folder_id && !hierarchy_movement_from_registered_parent {
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
}

pub(in crate::mapi) fn notification_registration_from_request(
    request: &RopRequest,
    logon_id: u8,
) -> MapiNotificationRegistration {
    let notification_types = request.notification_types().unwrap_or(0);
    let folder_id = if request.notification_want_whole_store().unwrap_or(true) {
        None
    } else {
        request.notification_folder_id()
    };
    MapiNotificationRegistration {
        logon_id,
        notification_types,
        folder_id,
    }
}
