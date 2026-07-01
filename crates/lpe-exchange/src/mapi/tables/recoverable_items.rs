use super::*;

pub(in crate::mapi) fn serialize_recoverable_item_row(
    item: &crate::mapi_store::MapiRecoverableItemMessage,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match canonical_property_storage_tag(*column) {
            PID_TAG_MID => write_object_id(&mut row, item.id),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                write_utf16z(&mut row, &item.item.subject)
            }
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPM.Note"),
            PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_rfc3339_utc(&item.item.received_at),
            ),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_MESSAGE_ACCESS),
            PID_TAG_MESSAGE_FLAGS => write_u32(&mut row, MSGFLAG_READ),
            PID_TAG_READ => row.push(1),
            PID_TAG_MESSAGE_SIZE => write_u32(
                &mut row,
                item.item.size_octets.clamp(0, u32::MAX as i64) as u32,
            ),
            PID_TAG_MESSAGE_SIZE_EXTENDED => {
                write_u64(&mut row, item.item.size_octets.max(0) as u64)
            }
            PID_TAG_SENDER_NAME_W | PID_TAG_SENDER_EMAIL_ADDRESS_W => {
                write_utf16z(&mut row, &item.item.sender_address)
            }
            PID_TAG_DISPLAY_TO_W | PID_TAG_DISPLAY_CC_W | PID_TAG_BODY_W => {
                write_utf16z(&mut row, "")
            }
            PID_TAG_HAS_ATTACHMENTS => row.push(item.item.has_attachments as u8),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(item.id),
            ),
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(item.id),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(item.folder_id),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(
                    mapi_mailstore::change_number_for_store_id(item.id),
                ),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(
                    mapi_mailstore::change_number_for_store_id(item.id),
                ),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(
                &mut row,
                mapi_mailstore::change_number_for_store_id(item.id),
            ),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(super) fn recoverable_item_property_value(
    item: &crate::mapi_store::MapiRecoverableItemMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    let change_number = mapi_mailstore::change_number_for_store_id(item.id);
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(item.id)),
        PID_TAG_INSTANCE_NUM | PID_TAG_DEPTH => Some(MapiValue::U32(0)),
        PID_TAG_ROW_TYPE => Some(MapiValue::U32(TABLE_LEAF_ROW)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(item.item.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Note".to_string())),
        PID_TAG_MESSAGE_DELIVERY_TIME
        | PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::I64(
            mapi_mailstore::filetime_from_rfc3339_utc(&item.item.received_at) as i64,
        )),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_READ => Some(MapiValue::Bool(true)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::U32(
            item.item.size_octets.clamp(0, u32::MAX as i64) as u32,
        )),
        PID_TAG_MESSAGE_SIZE_EXTENDED => {
            Some(mapi_message_size_extended_value(item.item.size_octets))
        }
        PID_TAG_SENDER_NAME_W | PID_TAG_SENDER_EMAIL_ADDRESS_W => {
            Some(MapiValue::String(item.item.sender_address.clone()))
        }
        PID_TAG_DISPLAY_TO_W | PID_TAG_DISPLAY_CC_W | PID_TAG_BODY_W => {
            Some(MapiValue::String(String::new()))
        }
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(item.item.has_attachments)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item.id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            item.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(item.folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}
