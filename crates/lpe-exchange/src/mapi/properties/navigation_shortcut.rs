use super::*;

pub(in crate::mapi) fn navigation_shortcut_property_value(
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    navigation_shortcut_property_value_with_store_entry_id(message, account_id, None, property_tag)
}

pub(in crate::mapi) fn navigation_shortcut_property_value_for_principal(
    message: &MapiNavigationShortcutMessage,
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Option<MapiValue> {
    let store_entry_id = crate::mapi::identity::principal_mailbox_store_entry_id(principal);
    navigation_shortcut_property_value_with_store_entry_id(
        message,
        principal.account_id,
        Some(&store_entry_id),
        property_tag,
    )
}

pub(in crate::mapi) fn navigation_shortcut_mutation_properties(
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
) -> HashMap<u32, MapiValue> {
    [
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_WLINK_ENTRY_ID,
        PID_TAG_WLINK_SAVE_STAMP,
        PID_TAG_WLINK_TYPE,
        PID_TAG_WLINK_FLAGS,
        PID_TAG_WLINK_SECTION,
        PID_TAG_WLINK_ORDINAL,
        PID_TAG_WLINK_FOLDER_TYPE,
        PID_TAG_WLINK_GROUP_HEADER_ID,
        PID_TAG_WLINK_GROUP_CLSID,
        PID_TAG_WLINK_GROUP_NAME_W,
        PID_TAG_WLINK_CALENDAR_COLOR,
        PID_TAG_WLINK_ADDRESS_BOOK_EID,
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
        PID_TAG_WLINK_CLIENT_ID,
        PID_TAG_WLINK_RO_GROUP_TYPE,
    ]
    .into_iter()
    .filter_map(|tag| {
        navigation_shortcut_property_value(message, account_id, tag).map(|v| (tag, v))
    })
    .collect()
}

pub(in crate::mapi) fn navigation_shortcut_with_pending_properties(
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
    pending_properties: &HashMap<u32, MapiValue>,
    deleted_properties: &HashSet<u32>,
) -> MapiNavigationShortcutMessage {
    if pending_properties.is_empty() && deleted_properties.is_empty() {
        return message.clone();
    }
    let properties = navigation_shortcut_properties_with_pending(
        message,
        account_id,
        pending_properties,
        deleted_properties,
    );
    let updated = navigation_shortcut_from_mapi_properties(
        account_id,
        Some(message.canonical_id),
        &properties,
    );
    MapiNavigationShortcutMessage {
        id: message.id,
        folder_id: message.folder_id,
        canonical_id: message.canonical_id,
        durable_identity: message.durable_identity.clone(),
        subject: updated.subject,
        target_folder_id: updated.target_folder_id,
        shortcut_type: updated.shortcut_type,
        flags: updated.flags,
        save_stamp: updated.save_stamp,
        section: updated.section,
        ordinal: updated.ordinal,
        group_header_id: updated.group_header_id,
        group_name: updated.group_name,
        client_properties: updated.client_properties,
    }
}

pub(in crate::mapi) fn navigation_shortcut_properties_with_pending(
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
    pending_properties: &HashMap<u32, MapiValue>,
    deleted_properties: &HashSet<u32>,
) -> HashMap<u32, MapiValue> {
    let mut properties = navigation_shortcut_mutation_properties(message, account_id);
    for tag in deleted_properties {
        properties.remove(&canonical_property_storage_tag(*tag));
    }
    properties.extend(
        pending_properties
            .iter()
            .map(|(tag, value)| (canonical_property_storage_tag(*tag), value.clone())),
    );
    properties
}

pub(in crate::mapi) fn navigation_shortcut_object_property_is_deleted(
    object: Option<&MapiObject>,
    property_tag: u32,
) -> bool {
    matches!(
        object,
        Some(MapiObject::NavigationShortcut {
            deleted_properties,
            ..
        }) if deleted_properties.contains(&canonical_property_storage_tag(property_tag))
    )
}

fn navigation_shortcut_property_value_with_store_entry_id(
    message: &MapiNavigationShortcutMessage,
    account_id: Uuid,
    store_entry_id: Option<&[u8]>,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    match property_tag {
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_MID => Some(MapiValue::U64(message.id)),
        PID_TAG_INST_ID => Some(MapiValue::U64(message.id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            message.folder_id,
            message.id,
        )
        .map(MapiValue::Binary),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(message.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            "IPM.Microsoft.WunderBar.Link".to_string(),
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_FAI)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I32(128)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(MapiValue::I64(128)),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_SOURCE_KEY | PID_TAG_RECORD_KEY => Some(MapiValue::Binary(
            message
                .durable_identity
                .as_ref()
                .map(|identity| identity.source_key.clone())
                .unwrap_or_else(|| mapi_mailstore::source_key_for_store_id(message.id)),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            message
                .durable_identity
                .as_ref()
                .map(|identity| identity.change_key.clone())
                .unwrap_or_else(|| {
                    mapi_mailstore::change_key_for_change_number(
                        mapi_mailstore::change_number_for_store_id(message.id),
                    )
                }),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            message
                .durable_identity
                .as_ref()
                .map(|identity| identity.predecessor_change_list.clone())
                .unwrap_or_else(|| {
                    mapi_mailstore::predecessor_change_list(
                        mapi_mailstore::change_number_for_store_id(message.id),
                    )
                }),
        )),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_PARENT_ENTRY_ID => {
            crate::mapi::identity::folder_entry_id_from_object_id(account_id, message.folder_id)
                .map(MapiValue::Binary)
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(
            message
                .durable_identity
                .as_ref()
                .map(|identity| identity.change_number)
                .unwrap_or_else(|| mapi_mailstore::change_number_for_store_id(message.id)),
        )),
        PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            message
                .durable_identity
                .as_ref()
                .map(|identity| identity.last_modification_time)
                .unwrap_or_else(|| {
                    mapi_mailstore::filetime_from_change_number(
                        mapi_mailstore::change_number_for_store_id(message.id),
                    )
                }),
        )),
        PID_TAG_WLINK_SAVE_STAMP => Some(MapiValue::U32(wlink_save_stamp(message))),
        PID_TAG_WLINK_TYPE => Some(MapiValue::U32(message.shortcut_type)),
        PID_TAG_WLINK_FLAGS => Some(MapiValue::U32(message.flags)),
        PID_TAG_WLINK_SECTION => Some(MapiValue::U32(message.section)),
        // [MS-OXOCFG] section 2.2.9.7: WlinkOrdinal is a variable-length
        // PtypBinary value whose complete byte string defines lexical order.
        PID_TAG_WLINK_ORDINAL => Some(MapiValue::Binary(message.ordinal.clone())),
        // [MS-OXOCFG] sections 2.2.9.3, 2.2.9.11, and 2.2.9.12
        // define these identifiers as exact 16-byte PtypBinary values.
        PID_TAG_WLINK_GROUP_HEADER_ID if message.shortcut_type == 4 => message
            .group_header_id
            .map(|group_id| MapiValue::Binary(group_id.as_bytes().to_vec())),
        PID_TAG_WLINK_GROUP_CLSID if message.shortcut_type != 4 => message
            .group_header_id
            .map(|group_id| MapiValue::Binary(group_id.as_bytes().to_vec())),
        // Section 3.1.4.10.1 does not list GroupName on group headers. Their
        // canonical display name is PidTagNormalizedSubject; only child
        // shortcuts carry the redundant group name from section 2.2.9.13.
        // The exact Mail-favorite shape retained by snapshot.rs represents
        // Outlook's observed omission by a missing group UUID and empty name.
        PID_TAG_WLINK_GROUP_NAME_W
            if message.shortcut_type != 4
                && !(message.shortcut_type == 0
                    && message.section == 1
                    && message.target_folder_id.is_some()
                    && message.group_header_id.is_none()
                    && message.group_name.trim().is_empty()) =>
        {
            Some(MapiValue::String(wlink_group_name(message)))
        }
        PID_TAG_WLINK_ENTRY_ID if message.shortcut_type != 4 => message
            .target_folder_id
            .and_then(|folder_id| {
                crate::mapi::identity::folder_entry_id_from_object_id(account_id, folder_id)
            })
            .map(MapiValue::Binary),
        property_tag
            if is_sharing_local_folder_id_property_tag(property_tag)
                && message.shortcut_type != 4 =>
        {
            message
                .target_folder_id
                .and_then(|folder_id| {
                    crate::mapi::identity::folder_entry_id_from_object_id(account_id, folder_id)
                })
                .map(MapiValue::Binary)
        }
        PID_TAG_WLINK_RECORD_KEY if message.shortcut_type != 4 => message
            .target_folder_id
            .map(mapi_mailstore::source_key_for_store_id)
            .map(MapiValue::Binary),
        PID_TAG_WLINK_STORE_ENTRY_ID if message.shortcut_type != 4 => {
            store_entry_id.map(|value| MapiValue::Binary(value.to_vec()))
        }
        // [MS-OXOCFG] sections 2.2.9.15 through 2.2.9.19 and 3.1.4.10.2:
        // these optional properties are written by the client. Replay exactly
        // the canonical stored values and do not synthesize missing values.
        PID_TAG_WLINK_CALENDAR_COLOR => {
            message.client_properties.calendar_color.map(MapiValue::I32)
        }
        PID_TAG_WLINK_ADDRESS_BOOK_EID => message
            .client_properties
            .address_book_entry_id
            .clone()
            .map(MapiValue::Binary),
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID => message
            .client_properties
            .address_book_store_entry_id
            .clone()
            .map(MapiValue::Binary),
        PID_TAG_WLINK_CLIENT_ID => message
            .client_properties
            .client_id
            .clone()
            .map(MapiValue::Binary),
        PID_TAG_WLINK_RO_GROUP_TYPE => message.client_properties.ro_group_type.map(MapiValue::I32),
        PID_TAG_WLINK_FOLDER_TYPE => {
            Some(MapiValue::Binary(wlink_folder_type_guid(message).to_vec()))
        }
        _ => None,
    }
}

fn is_sharing_local_folder_id_property_tag(property_tag: u32) -> bool {
    matches!(
        property_tag,
        PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG
            | OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG
    )
}
