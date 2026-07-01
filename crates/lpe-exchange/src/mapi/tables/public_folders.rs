use super::*;

pub(in crate::mapi) fn serialize_public_folder_item_row(
    item: &MapiPublicFolderItem,
    columns: &[u32],
) -> Vec<u8> {
    let change_number = mapi_mailstore::change_number_for_store_id(item.id);
    let message_class = if item.item.message_class.trim().is_empty() {
        "IPM.Post"
    } else {
        item.item.message_class.as_str()
    };
    let body_text = item.item.body_text.as_str();
    let mut row = Vec::new();
    for column in columns {
        match canonical_property_storage_tag(*column) {
            PID_TAG_MID => write_object_id(&mut row, item.id),
            PID_TAG_INST_ID => write_u64(&mut row, item.id),
            PID_TAG_INSTANCE_NUM => write_u32(&mut row, 0),
            PID_TAG_ROW_TYPE => write_u32(&mut row, TABLE_LEAF_ROW),
            PID_TAG_DEPTH => write_u32(&mut row, 0),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                write_utf16z(&mut row, &item.item.subject)
            }
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_MESSAGE_ACCESS),
            PID_TAG_MESSAGE_FLAGS => write_u32(&mut row, 0),
            PID_TAG_READ => row.push(item.item.is_read as u8),
            PID_TAG_MESSAGE_SIZE => write_u32(
                &mut row,
                body_text
                    .len()
                    .saturating_add(item.item.subject.len())
                    .min(u32::MAX as usize) as u32,
            ),
            PID_TAG_MESSAGE_SIZE_EXTENDED => write_u64(
                &mut row,
                body_text
                    .len()
                    .saturating_add(item.item.subject.len())
                    .min(i64::MAX as usize) as u64,
            ),
            PID_TAG_HAS_ATTACHMENTS => row.push(0),
            PID_TAG_BODY_W => write_utf16z(&mut row, body_text),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(item.id),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(item.folder_id),
            ),
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(item.id),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(change_number),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(change_number),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, change_number),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn public_folder_item_size(item: &MapiPublicFolderItem) -> i64 {
    item.item
        .body_text
        .len()
        .saturating_add(item.item.subject.len())
        .min(i64::MAX as usize) as i64
}

pub(super) fn public_folder_item_property_value(
    item: &MapiPublicFolderItem,
    property_tag: u32,
) -> Option<MapiValue> {
    let change_number = mapi_mailstore::change_number_for_store_id(item.id);
    let message_class = if item.item.message_class.trim().is_empty() {
        "IPM.Post"
    } else {
        item.item.message_class.as_str()
    };
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(item.id)),
        PID_TAG_INSTANCE_NUM | PID_TAG_DEPTH => Some(MapiValue::U32(0)),
        PID_TAG_ROW_TYPE => Some(MapiValue::U32(TABLE_LEAF_ROW)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(item.item.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(message_class.to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0)),
        PID_TAG_READ => Some(MapiValue::Bool(item.item.is_read)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(public_folder_item_size(item))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
            public_folder_item_size(item),
        )),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_BODY_W => Some(MapiValue::String(item.item.body_text.clone())),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item.id),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(item.folder_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            item.id,
        ))),
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
