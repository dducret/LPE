use super::*;

pub(super) fn delegate_freebusy_sync_object(
    message: &crate::mapi_store::MapiDelegateFreeBusyMessage,
    mailbox_guid: Uuid,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let local_freebusy = crate::mapi_store::is_outlook_local_freebusy_message(message);
    let (source_key, change_key, predecessor_change_list, change_number, last_modified_filetime) =
        if local_freebusy {
            let identity = message
                .durable_identity
                .as_ref()
                .expect("canonical LocalFreebusy has a durable MAPI identity");
            (
                identity.source_key.clone(),
                identity.change_key.clone(),
                identity.predecessor_change_list.clone(),
                identity.change_number,
                identity.last_modification_time,
            )
        } else {
            let change_number = mapi_mailstore::change_number_for_store_id(message.id);
            (
                mapi_mailstore::source_key_for_store_id(message.id),
                mapi_mailstore::change_key_for_change_number(change_number),
                mapi_mailstore::predecessor_change_list(change_number),
                change_number,
                mapi_mailstore::filetime_from_change_number(change_number),
            )
        };
    let message_size = message
        .message
        .subject
        .len()
        .saturating_add(message.message.body_text.len())
        .saturating_add(message.message.payload_json.len())
        .min(i64::MAX as usize) as i64;
    let mut named_properties = vec![
        (
            PID_TAG_MESSAGE_FLAGS,
            mapi_mailstore::SpecialMessagePropertyValue::U32(
                if delegate_freebusy_message_is_associated(message) {
                    0x0000_0040
                } else {
                    0
                },
            ),
        ),
        (
            PID_TAG_SOURCE_KEY,
            mapi_mailstore::SpecialMessagePropertyValue::Binary(source_key),
        ),
        (
            PID_TAG_CHANGE_KEY,
            mapi_mailstore::SpecialMessagePropertyValue::Binary(change_key),
        ),
        (
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            mapi_mailstore::SpecialMessagePropertyValue::Binary(predecessor_change_list),
        ),
        (
            PID_TAG_CHANGE_NUMBER,
            mapi_mailstore::SpecialMessagePropertyValue::U64(change_number),
        ),
    ];
    if local_freebusy {
        for property_tag in [
            0x6841_0003,
            0x6842_000B,
            0x6843_000B,
            0x684A_101F,
            0x6845_1102,
            0x686B_1003,
            0x6870_1102,
            0x6871_1003,
            0x6872_001F,
            0x686D_000B,
            0x686E_000B,
            0x686F_000B,
            0x684B_000B,
            0x6844_101F,
        ] {
            if let Some(value) =
                delegate_freebusy_property_value(message, mailbox_guid, property_tag)
                    .and_then(special_message_property_value)
            {
                named_properties.push((property_tag, value));
            }
        }
        for stored in &message.custom_properties {
            if stored.property_type != stored.property_tag as u16
                || !crate::mapi::dispatch::custom_properties::is_custom_property_tag(
                    stored.property_tag,
                )
            {
                continue;
            }
            let mut cursor = Cursor::new(&stored.property_value);
            let Ok(value) = parse_mapi_property_value(&mut cursor, stored.property_tag) else {
                continue;
            };
            if cursor.remaining() != 0 {
                continue;
            }
            if let Some(value) = special_message_property_value(value) {
                named_properties.push((stored.property_tag, value));
            }
        }
    } else {
        named_properties.push((
            0x6843_000B,
            mapi_mailstore::SpecialMessagePropertyValue::Bool(false),
        ));
    }

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: delegate_freebusy_message_is_associated(message),
        subject: message.message.subject.clone(),
        body_text: Some(message.message.body_text.clone()),
        message_class: if message.message.message_kind == "delegate" {
            "IPM.Microsoft.Delegate".to_string()
        } else {
            "IPM.Microsoft.ScheduleData.FreeBusy".to_string()
        },
        last_modified_filetime,
        message_size,
        read_state: None,
        recipients: Vec::new(),
        named_properties,
        named_property_definitions: HashMap::new(),
    }
}
