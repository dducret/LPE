use super::*;

pub(in crate::mapi) fn search_folder_definition_property_value(
    definition: &SearchFolderDefinition,
    folder_id: u64,
    property_tag: u32,
    mailbox_guid: Uuid,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let parent_folder_id = SEARCH_FOLDER_ID;
    let message_class =
        search_folder_container_class_for_result_kind(&definition.result_object_kind);
    let change_number = mapi_mailstore::change_number_for_store_id(folder_id);
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(definition.display_name.clone())),
        PID_TAG_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_RECORD_KEY | PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(folder_id),
        )),
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder_id)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(parent_folder_id)),
        PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_SEARCH)),
        PID_TAG_CONTENT_COUNT
        | PID_TAG_CONTENT_UNREAD_COUNT
        | PID_TAG_DELETED_COUNT_TOTAL
        | PID_TAG_ASSOCIATED_CONTENT_COUNT => Some(MapiValue::U32(0)),
        PID_TAG_ACCESS | PID_TAG_RIGHTS => Some(MapiValue::U32(MAPI_FOLDER_ACCESS)),
        PID_TAG_EXTENDED_FOLDER_FLAGS => Some(MapiValue::Binary(
            extended_folder_flags_for_search_folder(definition, folder_id),
        )),
        PID_TAG_SEARCH_FOLDER_ID => Some(MapiValue::Binary(search_folder_id(definition))),
        PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS | PID_TAG_ARCHIVE_PERIOD => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
            if default_view_supported_folder(folder_id, message_class) =>
        {
            default_folder_view_entry_id(mailbox_guid, folder_id, message_class)
        }
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(false)),
        PID_TAG_ATTRIBUTE_HIDDEN => Some(MapiValue::Bool(false)),
        PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W => {
            Some(MapiValue::String(message_class.to_string()))
        }
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 | PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
            default_post_message_class_for_container_class(message_class)
                .map(|default_class| MapiValue::String(default_class.to_string()))
        }
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_LOCAL_COMMIT_TIME_MAX
        | PID_TAG_HIER_REV => Some(MapiValue::I64(mapi_mailstore::filetime_from_change_number(
            change_number,
        ) as i64)),
        PID_TAG_HIERARCHY_CHANGE_NUMBER => {
            Some(MapiValue::U32(change_number.min(u64::from(u32::MAX)) as u32))
        }
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(parent_folder_id),
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

fn search_folder_container_class_for_result_kind(result_object_kind: &str) -> &'static str {
    match result_object_kind {
        "contact" => "IPF.Contact",
        "task" => "IPF.Task",
        "mixed" | "message" => "IPF.Note",
        _ => "IPF.Note",
    }
}

pub(in crate::mapi) fn search_folder_definition_message_property_value(
    definition: &SearchFolderDefinition,
    account_id: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let message_id = search_folder_definition_message_id(definition)?;
    let change_number = mapi_mailstore::change_number_for_store_id(message_id);
    match property_tag {
        PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID => {
            Some(MapiValue::U64(COMMON_VIEWS_FOLDER_ID))
        }
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(message_id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            COMMON_VIEWS_FOLDER_ID,
            message_id,
        )
        .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message_id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(definition.display_name.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            "IPM.Microsoft.WunderBar.SFInfo".to_string(),
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_FAI)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(128)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(MapiValue::I64(128)),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_SOURCE_KEY | PID_TAG_RECORD_KEY | PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_uuid(&definition.id),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(COMMON_VIEWS_FOLDER_ID),
        )),
        PID_TAG_PARENT_ENTRY_ID => crate::mapi::identity::folder_entry_id_from_object_id(
            account_id,
            COMMON_VIEWS_FOLDER_ID,
        )
        .map(MapiValue::Binary),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        PID_TAG_SEARCH_FOLDER_ID => Some(MapiValue::Binary(search_folder_id(definition))),
        PID_TAG_SEARCH_FOLDER_TEMPLATE_ID => {
            Some(MapiValue::U32(search_folder_template_id(definition)))
        }
        PID_TAG_SEARCH_FOLDER_TAG => Some(MapiValue::U32(search_folder_tag(definition))),
        PID_TAG_SEARCH_FOLDER_LAST_USED => Some(MapiValue::U32(search_folder_last_used())),
        PID_TAG_SEARCH_FOLDER_EXPIRATION => Some(MapiValue::U32(search_folder_expiration())),
        PID_TAG_SEARCH_FOLDER_STORAGE_TYPE => {
            Some(MapiValue::U32(search_folder_storage_type(definition)))
        }
        PID_TAG_SEARCH_FOLDER_EFP_FLAGS => Some(MapiValue::U32(0)),
        PID_TAG_SEARCH_FOLDER_DEFINITION => {
            Some(MapiValue::Binary(search_folder_definition_blob(definition)))
        }
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_change_number(change_number),
        )),
        _ => None,
    }
}

fn search_folder_definition_message_id(definition: &SearchFolderDefinition) -> Option<u64> {
    crate::mapi::identity::mapped_mapi_object_id(&definition.id)
}

fn search_folder_template_id(definition: &SearchFolderDefinition) -> u32 {
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderTemplateId")
        .and_then(serde_json::Value::as_u64)
    {
        return value.min(u64::from(u32::MAX)) as u32;
    }
    match definition.role.as_str() {
        "todo_search" => 10,
        _ if definition.is_builtin => 2,
        _ => 0,
    }
}

fn search_folder_storage_type(definition: &SearchFolderDefinition) -> u32 {
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderStorageType")
        .and_then(serde_json::Value::as_u64)
    {
        return value.min(u64::from(u32::MAX)) as u32;
    }
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderDefinition")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| BASE64_STANDARD.decode(value).ok())
        .and_then(|blob| {
            blob.get(4..8)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_le_bytes)
        })
    {
        return value;
    }
    let mut storage_type = 0x48;
    if search_folder_text_search(definition).is_some() {
        storage_type |= 0x02;
    }
    if search_folder_numerical_search(definition).is_some() {
        storage_type |= 0x01;
    }
    storage_type
}

fn search_folder_definition_blob(definition: &SearchFolderDefinition) -> Vec<u8> {
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderDefinition")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| BASE64_STANDARD.decode(value).ok())
    {
        return value;
    }
    let mut blob = Vec::new();
    blob.extend_from_slice(&0x0000_1004u32.to_le_bytes());
    blob.extend_from_slice(&search_folder_storage_type(definition).to_le_bytes());
    blob.extend_from_slice(&search_folder_numerical_search(definition).unwrap_or([0; 4]));
    write_search_folder_text_search(&mut blob, search_folder_text_search(definition).as_deref());
    blob.extend_from_slice(&0u32.to_le_bytes());
    blob.extend_from_slice(
        &(definition
            .scope_json
            .get("recursive")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(definition.is_builtin) as u32)
            .to_le_bytes(),
    );
    blob.push(0);
    blob.extend_from_slice(&0u32.to_le_bytes());
    blob.extend_from_slice(&0u32.to_le_bytes());
    blob
}

fn search_folder_text_search(definition: &SearchFolderDefinition) -> Option<String> {
    definition
        .restriction_json
        .get("pidTagSearchFolderTextSearch")
        .or_else(|| definition.restriction_json.get("textSearch"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(u16::MAX as usize).collect())
}

fn search_folder_numerical_search(definition: &SearchFolderDefinition) -> Option<[u8; 4]> {
    if let Some(age) = definition
        .restriction_json
        .get("pidTagSearchFolderNumericalSearchAge")
    {
        let unit = age
            .get("unit")
            .and_then(serde_json::Value::as_str)
            .and_then(|unit| match unit {
                "days" => Some(0u32),
                "weeks" => Some(1u32),
                "months" => Some(2u32),
                _ => None,
            })?;
        let amount = age
            .get("amount")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default()
            .min(u64::from(u16::MAX)) as u32;
        return Some(((unit << 16) | amount).to_be_bytes());
    }

    definition
        .restriction_json
        .get("pidTagSearchFolderNumericalSearch")
        .or_else(|| definition.restriction_json.get("numericalSearch"))
        .and_then(serde_json::Value::as_u64)
        .map(|value| (value.min(u64::from(u32::MAX)) as u32).to_le_bytes())
}

fn write_search_folder_text_search(blob: &mut Vec<u8>, text: Option<&str>) {
    let Some(text) = text else {
        blob.push(0);
        return;
    };
    let char_count = text.chars().count().min(u16::MAX as usize);
    if char_count > 254 {
        blob.push(255);
        blob.extend_from_slice(&(char_count as u16).to_le_bytes());
    } else {
        blob.push(char_count as u8);
    }
    blob.extend_from_slice(text.as_bytes());
}

fn search_folder_last_used() -> u32 {
    214_089_600
}

fn search_folder_expiration() -> u32 {
    214_089_641
}

pub(in crate::mapi) fn search_folder_tag(definition: &SearchFolderDefinition) -> u32 {
    if let Some(value) = definition
        .restriction_json
        .get("pidTagSearchFolderTag")
        .and_then(serde_json::Value::as_u64)
    {
        return value.min(u64::from(u32::MAX)) as u32;
    }
    let bytes = definition.id.as_bytes();
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}
