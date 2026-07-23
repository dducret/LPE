use super::*;

#[derive(Clone)]
pub(super) enum AssociatedTableRow {
    Config(MapiAssociatedConfigMessage),
}

pub(in crate::mapi) fn serialize_navigation_shortcut_row(
    message: &MapiNavigationShortcutMessage,
    principal: Option<&AccountPrincipal>,
    columns: &[u32],
) -> Vec<u8> {
    let account_id = principal
        .map(|principal| principal.account_id)
        .unwrap_or_default();
    let mut row = Vec::new();
    for column in columns {
        let value = principal
            .and_then(|principal| {
                navigation_shortcut_property_value_for_principal(message, principal, *column)
            })
            .or_else(|| navigation_shortcut_property_value(message, account_id, *column));
        match value {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(super) fn serialize_common_views_property_row_with_mailbox_guid(
    message: &MapiCommonViewsMessage,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    serialize_optional_property_row(
        columns,
        columns
            .iter()
            .map(|column| common_views_message_property_value(message, mailbox_guid, *column))
            .collect(),
    )
}

pub(super) fn serialize_common_views_property_row_for_principal(
    message: &MapiCommonViewsMessage,
    principal: &AccountPrincipal,
    columns: &[u32],
) -> Vec<u8> {
    serialize_optional_property_row(
        columns,
        columns
            .iter()
            .map(|column| {
                common_views_message_property_value_for_principal(message, principal, *column)
            })
            .collect(),
    )
}

pub(in crate::mapi) fn serialize_search_folder_definition_row_with_mailbox_guid(
    message: &SearchFolderDefinition,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match search_folder_definition_message_property_value(message, mailbox_guid, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_common_view_named_view_row_with_mailbox_guid(
    message: &MapiCommonViewNamedViewMessage,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match common_view_named_view_property_value(message, mailbox_guid, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_conversation_action_row(
    message: &MapiConversationActionMessage,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match conversation_action_property_value(message, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_delegate_freebusy_row(
    message: &MapiDelegateFreeBusyMessage,
    columns: &[u32],
) -> Vec<u8> {
    serialize_freebusy_row_staged(message, columns, None)
}

pub(in crate::mapi) fn serialize_freebusy_row_staged(
    message: &MapiDelegateFreeBusyMessage,
    columns: &[u32],
    pending_appointment_tombstone: Option<&[u8]>,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        let value = if crate::mapi_store::is_outlook_local_freebusy_message_id(message.id)
            && canonical_property_storage_tag(*column)
                == PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE
        {
            Some(MapiValue::Binary(
                pending_appointment_tombstone
                    .unwrap_or(&EMPTY_APPOINTMENT_TOMBSTONE)
                    .to_vec(),
            ))
        } else {
            delegate_freebusy_property_value(message, *column)
        };
        match value {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_associated_config_row_with_mailbox_guid(
    message: &MapiAssociatedConfigMessage,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match associated_config_property_value_with_mailbox_guid(message, mailbox_guid, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(super) fn has_associated_table_rows(folder_id: u64, snapshot: &MapiMailStoreSnapshot) -> bool {
    !snapshot
        .associated_config_messages_for_folder(folder_id)
        .is_empty()
}

pub(super) fn should_use_associated_config_table(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    _restriction: Option<&MapiRestriction>,
) -> bool {
    has_associated_table_rows(folder_id, snapshot)
}

pub(super) fn associated_table_rows(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    _mailbox_guid: Uuid,
) -> Vec<AssociatedTableRow> {
    snapshot
        .associated_config_messages_for_folder(folder_id)
        .into_iter()
        .filter(|message| {
            restriction_matches_associated_config(restriction, message)
                && associated_config_visible_in_table(folder_id, restriction, message)
        })
        .map(AssociatedTableRow::Config)
        .collect()
}

pub(in crate::mapi) fn associated_config_visible_in_table(
    _folder_id: u64,
    _restriction: Option<&MapiRestriction>,
    message: &MapiAssociatedConfigMessage,
) -> bool {
    !crate::mapi_store::is_outlook_inbox_virtual_only_associated_config_id(message.id)
}

#[cfg(test)]
pub(super) fn outlook_configuration_prefix_restriction() -> MapiRestriction {
    MapiRestriction::Content {
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: "IPM.Configuration.".to_string(),
        fuzzy_level_low: 0x0002,
        fuzzy_level_high: 0x0001,
    }
}

pub(super) fn serialize_associated_table_property_row(
    message: &AssociatedTableRow,
    mailbox_guid: Uuid,
    columns: &[u32],
) -> Vec<u8> {
    serialize_optional_property_row(
        columns,
        columns
            .iter()
            .map(|column| associated_table_row_property_value(message, mailbox_guid, *column))
            .collect(),
    )
}

fn serialize_optional_property_row(columns: &[u32], values: Vec<Option<MapiValue>>) -> Vec<u8> {
    // [MS-OXCDATA] sections 2.8.1.2 and 2.11.5 require a
    // FlaggedPropertyRow when any selected property is absent.
    let flagged = values.iter().any(Option::is_none);
    let mut row = vec![u8::from(flagged)];
    for (column, value) in columns.iter().zip(values) {
        if let Some(value) = value {
            if flagged {
                row.push(0);
            }
            write_mapi_value(&mut row, *column, &value);
        } else {
            row.push(0x0A);
            write_u32(&mut row, 0x8004_010F);
        }
    }
    row
}

pub(super) fn associated_table_row_property_value(
    message: &AssociatedTableRow,
    mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    match message {
        AssociatedTableRow::Config(message) => {
            associated_config_property_value_with_mailbox_guid(message, mailbox_guid, property_tag)
        }
    }
}

pub(super) fn associated_table_row_matches(
    message: &AssociatedTableRow,
    restriction: Option<&MapiRestriction>,
    _mailbox_guid: Uuid,
) -> bool {
    match message {
        AssociatedTableRow::Config(message) => {
            restriction_matches_associated_config(restriction, message)
        }
    }
}

pub(super) fn associated_table_row_config(
    message: &AssociatedTableRow,
) -> Option<&MapiAssociatedConfigMessage> {
    match message {
        AssociatedTableRow::Config(message) => Some(message),
    }
}

pub(super) fn associated_table_row_id(message: &AssociatedTableRow) -> u64 {
    match message {
        AssociatedTableRow::Config(message) => message.id,
    }
}

pub(in crate::mapi) fn restriction_matches_common_views_message(
    restriction: Option<&MapiRestriction>,
    message: &MapiCommonViewsMessage,
    mailbox_guid: Uuid,
) -> bool {
    match message {
        MapiCommonViewsMessage::NavigationShortcut(shortcut) => {
            restriction_matches_navigation_shortcut(restriction, shortcut, mailbox_guid)
        }
        MapiCommonViewsMessage::NamedView(view) => {
            restriction_matches_common_view_named_view(restriction, view, mailbox_guid)
        }
        MapiCommonViewsMessage::SearchFolderDefinition(definition) => {
            restriction_matches(restriction, |property_tag| {
                search_folder_definition_message_property_value(
                    definition,
                    mailbox_guid,
                    property_tag,
                )
            })
        }
        MapiCommonViewsMessage::AssociatedConfig(message) => {
            restriction_matches_associated_config(restriction, message)
        }
    }
}

pub(super) fn common_views_message_property_value(
    message: &MapiCommonViewsMessage,
    mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    match message {
        MapiCommonViewsMessage::NavigationShortcut(message) => {
            navigation_shortcut_property_value(message, mailbox_guid, property_tag)
        }
        MapiCommonViewsMessage::NamedView(message) => {
            common_view_named_view_property_value(message, mailbox_guid, property_tag)
        }
        MapiCommonViewsMessage::SearchFolderDefinition(message) => {
            search_folder_definition_message_property_value(message, mailbox_guid, property_tag)
        }
        MapiCommonViewsMessage::AssociatedConfig(message) => {
            associated_config_property_value_with_mailbox_guid(message, mailbox_guid, property_tag)
        }
    }
}

fn common_views_message_property_value_for_principal(
    message: &MapiCommonViewsMessage,
    principal: &AccountPrincipal,
    property_tag: u32,
) -> Option<MapiValue> {
    match message {
        MapiCommonViewsMessage::NavigationShortcut(message) => {
            navigation_shortcut_property_value_for_principal(message, principal, property_tag)
        }
        MapiCommonViewsMessage::NamedView(message) => {
            common_view_named_view_property_value(message, principal.account_id, property_tag)
        }
        MapiCommonViewsMessage::SearchFolderDefinition(message) => {
            search_folder_definition_message_property_value(
                message,
                principal.account_id,
                property_tag,
            )
        }
        MapiCommonViewsMessage::AssociatedConfig(message) => {
            associated_config_property_value_with_mailbox_guid(
                message,
                principal.account_id,
                property_tag,
            )
        }
    }
}

fn associated_config_message_size(message: &MapiAssociatedConfigMessage) -> i64 {
    message
        .subject
        .len()
        .saturating_add(message.message_class.len())
        .saturating_add(message.properties_json.to_string().len())
        .min(i64::MAX as usize) as i64
}

fn sanitize_configuration_property_value(
    message_class: &str,
    property_tag: u32,
    value: MapiValue,
) -> MapiValue {
    if property_tag == PID_TAG_ROAMING_DICTIONARY
        && crate::mapi_store::is_outlook_configuration_message_class(message_class)
        && matches!(
            &value,
            MapiValue::Binary(bytes)
                if bytes == b"<xml/>" || is_stale_minimal_umolk_dictionary(message_class, bytes)
        )
    {
        return MapiValue::Binary(minimal_roaming_dictionary_stream());
    }
    value
}

pub(in crate::mapi) fn is_stale_minimal_umolk_dictionary(
    message_class: &str,
    bytes: &[u8],
) -> bool {
    crate::mapi_store::is_outlook_umolk_user_options_message_class(message_class)
        && bytes
            .windows(br#"Info version="LPE.1""#.len())
            .any(|window| window == br#"Info version="LPE.1""#)
        && bytes
            .windows(br#"18-OLPrefsVersion" v="9-0""#.len())
            .any(|window| window == br#"18-OLPrefsVersion" v="9-0""#)
}

pub(in crate::mapi) fn associated_config_property_value(
    message: &MapiAssociatedConfigMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    associated_config_property_value_with_mailbox_guid(message, Uuid::nil(), property_tag)
}

pub(in crate::mapi) fn associated_config_property_is_client_absent(
    message: Option<&MapiAssociatedConfigMessage>,
    property_tag: u32,
) -> bool {
    message.is_some_and(|message| {
        crate::mapi_store::is_outlook_configuration_message_class(&message.message_class)
            && mapi_properties_from_json(&message.properties_json)
                .contains_key(&PID_TAG_ROAMING_DATATYPES)
            && matches!(
                property_tag,
                PID_NAME_CONTENT_CLASS_W_TAG | PID_NAME_CONTENT_TYPE_W_TAG
            )
    })
}

pub(in crate::mapi) fn associated_config_modeled_empty_property(
    message: Option<&MapiAssociatedConfigMessage>,
    property_tag: u32,
) -> bool {
    let Some(message) = message else {
        return false;
    };
    if !matches!(
        message.message_class.as_str(),
        "IPM.Microsoft.ContactLink.TimeStamp" | "IPM.Microsoft.OSC.ContactSync"
    ) {
        return false;
    }
    let property_id = u32::from(MapiPropertyTag::new(property_tag).property_id());
    property_id == u32::from(MapiPropertyTag::new(PID_NAME_OSC_CONTACT_SOURCES_TAG).property_id())
        || matches!(
            property_id,
            PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1
                | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA
                | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC
                | PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED
        )
}

pub(in crate::mapi) fn associated_config_property_value_with_mailbox_guid(
    message: &MapiAssociatedConfigMessage,
    mailbox_guid: Uuid,
    property_tag: u32,
) -> Option<MapiValue> {
    let lookup_tag = canonical_property_storage_tag(property_tag);
    let properties = mapi_properties_from_json(&message.properties_json);
    properties
        .get(&lookup_tag)
        .cloned()
        .filter(|_| !crate::mapi_store::is_associated_config_read_only_property_tag(lookup_tag))
        .map(|value| {
            sanitize_configuration_property_value(&message.message_class, lookup_tag, value)
        })
        .or_else(|| {
            if associated_config_property_is_client_absent(Some(message), lookup_tag) {
                // The persisted client configuration establishes that this
                // compatibility property was not supplied; do not invent it.
                // Once absent, [MS-OXCDATA] sections 2.4.2, 2.8.1.2, and
                // 2.11.5 encode it as ecNotFound in a flagged property cell.
                return None;
            }
            if crate::mapi_store::is_outlook_umolk_user_options_message_class(
                &message.message_class,
            ) && !is_umolk_computed_property(lookup_tag)
            {
                return None;
            }
            let change_number = mapi_mailstore::change_number_for_store_id(message.id);
            match lookup_tag {
                PID_TAG_MID => Some(MapiValue::U64(message.id)),
                PID_TAG_INST_ID => Some(MapiValue::U64(message.id)),
                PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
                PID_TAG_ENTRY_ID => crate::mapi::identity::message_entry_id_from_object_ids(
                    mailbox_guid,
                    message.folder_id,
                    message.id,
                )
                .map(MapiValue::Binary),
                PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
                    crate::mapi::identity::instance_key_for_object_id(message.id),
                )),
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_CONVERSATION_TOPIC_W => {
                    Some(MapiValue::String(message.subject.clone()))
                }
                PID_TAG_MESSAGE_CLASS_W | PID_TAG_ORIGINAL_MESSAGE_CLASS_W => {
                    Some(MapiValue::String(message.message_class.clone()))
                }
                PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0x0000_0040)),
                PID_TAG_MESSAGE_STATUS => Some(MapiValue::U32(0)),
                PID_TAG_ACCESS_LEVEL => Some(MapiValue::U32(1)),
                PID_TAG_IMPORTANCE => Some(MapiValue::U32(1)),
                PID_TAG_PRIORITY | PID_TAG_SENSITIVITY | PID_TAG_ORIGINAL_SENSITIVITY => {
                    Some(MapiValue::U32(0))
                }
                PID_TAG_REPLY_REQUESTED
                | PID_TAG_RESPONSE_REQUESTED
                | PID_TAG_ALTERNATE_RECIPIENT_ALLOWED
                | PID_TAG_AUTO_FORWARDED
                | PID_TAG_DELETE_AFTER_SUBMIT
                | PID_TAG_HAS_ATTACHMENTS
                | PID_TAG_ORIGINATOR_DELIVERY_REPORT_REQUESTED
                | PID_TAG_PROCESSED
                | PID_TAG_READ
                | PID_TAG_READ_RECEIPT_REQUESTED
                | PID_TAG_RECIPIENT_REASSIGNMENT_PROHIBITED
                | PID_TAG_RTF_IN_SYNC => Some(MapiValue::Bool(false)),
                PID_TAG_ICON_INDEX
                | PID_TAG_INTERNET_MAIL_OVERRIDE_FORMAT
                | PID_TAG_LAST_VERB_EXECUTED
                | PID_TAG_MESSAGE_EDITOR_FORMAT
                | PID_TAG_OWNER_APPOINTMENT_ID => Some(MapiValue::U32(0)),
                PID_TAG_FLAG_STATUS => Some(MapiValue::U32(0)),
                PID_TAG_FOLLOWUP_ICON | PID_TAG_TODO_ITEM_FLAGS => Some(MapiValue::I32(0)),
                PID_TAG_RTF_COMPRESSED
                | PID_TAG_ORIGINAL_AUTHOR_ENTRY_ID
                | PID_TAG_PARENT_KEY
                | PID_TAG_REPLY_RECIPIENT_ENTRIES
                | PID_TAG_REPORT_TAG => Some(MapiValue::Binary(Vec::new())),
                PID_TAG_DISPLAY_BCC_W
                | PID_TAG_DISPLAY_CC_W
                | PID_TAG_DISPLAY_TO_W
                | PID_TAG_IN_REPLY_TO_ID_W
                | PID_TAG_INTERNET_REFERENCES_W
                | PID_TAG_NEXT_SEND_ACCOUNT_W
                | PID_TAG_ORIGINAL_AUTHOR_NAME_W
                | PID_TAG_ORIGINAL_DISPLAY_BCC_W
                | PID_TAG_ORIGINAL_DISPLAY_CC_W
                | PID_TAG_ORIGINAL_DISPLAY_TO_W
                | PID_TAG_PRIMARY_SEND_ACCOUNT_W
                | PID_TAG_REPLY_RECIPIENT_NAMES_W
                | PID_TAG_REPORT_DISPOSITION_W
                | PID_TAG_SUBJECT_PREFIX_W => Some(MapiValue::String(String::new())),
                PID_TAG_CLIENT_SUBMIT_TIME => Some(MapiValue::I64(
                    associated_config_last_modified_filetime(message).unwrap_or_else(|| {
                        mapi_mailstore::filetime_from_change_number(change_number)
                    }) as i64,
                )),
                PID_TAG_CREATION_TIME => Some(MapiValue::I64(
                    associated_config_creation_filetime(message).unwrap_or_else(|| {
                        associated_config_last_modified_filetime(message).unwrap_or_else(|| {
                            mapi_mailstore::filetime_from_change_number(change_number)
                        })
                    }) as i64,
                )),
                PID_TAG_LAST_MODIFIER_NAME_W => {
                    associated_config_last_modifier_name(message).map(MapiValue::String)
                }
                PID_TAG_DEFERRED_DELIVERY_TIME
                | PID_TAG_DEFERRED_SEND_TIME
                | PID_TAG_END_DATE
                | PID_TAG_EXPIRY_TIME
                | PID_TAG_FLAG_COMPLETE_TIME
                | PID_TAG_LAST_VERB_EXECUTION_TIME
                | PID_TAG_ORIGINAL_SUBMIT_TIME
                | PID_TAG_REPLY_TIME
                | PID_TAG_REPORT_TIME
                | PID_TAG_START_DATE => Some(MapiValue::I64(0)),
                PID_TAG_NATIVE_BODY => Some(MapiValue::U32(1)),
                PID_TAG_INTERNET_CODEPAGE => Some(MapiValue::U32(65001)),
                PID_TAG_MESSAGE_LOCALE_ID => Some(MapiValue::U32(0x0409)),
                PID_TAG_SENT_MAIL_SVR_EID => Some(MapiValue::Binary(Vec::new())),
                PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
                PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(
                    associated_config_message_size(message),
                )),
                PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
                    associated_config_message_size(message),
                )),
                PID_TAG_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
                PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
                PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(
                    mapi_mailstore::source_key_for_store_id(message.id),
                )),
                PID_TAG_RECORD_KEY => Some(MapiValue::Binary(
                    mapi_mailstore::source_key_for_store_id(message.id),
                )),
                PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(
                    mapi_mailstore::source_key_for_store_id(message.id),
                )),
                PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
                    mapi_mailstore::source_key_for_store_id(message.folder_id),
                )),
                PID_TAG_PARENT_ENTRY_ID => crate::mapi::identity::folder_entry_id_from_object_id(
                    mailbox_guid,
                    message.folder_id,
                )
                .map(MapiValue::Binary),
                PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
                    mapi_mailstore::change_key_for_change_number(change_number),
                )),
                PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
                    mapi_mailstore::predecessor_change_list(change_number),
                )),
                PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
                PID_TAG_LAST_MODIFICATION_TIME
                | PID_TAG_LOCAL_COMMIT_TIME
                | PID_TAG_MESSAGE_DELIVERY_TIME => Some(MapiValue::I64(
                    associated_config_last_modified_filetime(message).unwrap_or_else(|| {
                        mapi_mailstore::filetime_from_change_number(change_number)
                    }) as i64,
                )),
                PID_TAG_ROAMING_DATATYPES
                    if crate::mapi_store::is_outlook_configuration_message_class(
                        &message.message_class,
                    ) =>
                {
                    Some(MapiValue::U32(configuration_roaming_datatypes(
                        &message.message_class,
                        &properties,
                    )))
                }
                PID_TAG_ROAMING_DICTIONARY
                    if crate::mapi_store::is_outlook_configuration_message_class(
                        &message.message_class,
                    ) =>
                {
                    (!configuration_uses_xml_stream(&message.message_class)
                        && !properties.contains_key(&PID_TAG_ROAMING_DATATYPES))
                    .then(|| MapiValue::Binary(minimal_roaming_dictionary_stream()))
                }
                PID_TAG_ROAMING_XML_STREAM
                    if crate::mapi_store::is_outlook_configuration_message_class_name(
                        &message.message_class,
                        "IPM.Configuration.WorkHours",
                    ) =>
                {
                    (!properties.contains_key(&PID_TAG_ROAMING_DATATYPES))
                        .then(|| MapiValue::Binary(minimal_working_hours_roaming_xml_stream()))
                }
                PID_TAG_ROAMING_XML_STREAM
                    if crate::mapi_store::is_outlook_configuration_message_class_name(
                        &message.message_class,
                        "IPM.Configuration.CategoryList",
                    ) =>
                {
                    (!properties.contains_key(&PID_TAG_ROAMING_DATATYPES))
                        .then(|| MapiValue::Binary(minimal_category_list_roaming_xml_stream()))
                }
                PID_TAG_ROAMING_XML_STREAM
                    if crate::mapi_store::is_outlook_configuration_message_class_name(
                        &message.message_class,
                        "IPM.Configuration.MRM",
                    ) =>
                {
                    (!properties.contains_key(&PID_TAG_ROAMING_DATATYPES))
                        .then(|| MapiValue::Binary(minimal_mrm_roaming_xml_stream()))
                }
                PID_TAG_ROAMING_DATATYPES
                    if message.message_class
                        == crate::mapi_store::OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS =>
                {
                    Some(MapiValue::U32(0x0000_0002))
                }
                PID_TAG_ROAMING_XML_STREAM
                    if message.message_class
                        == crate::mapi_store::OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS =>
                {
                    Some(MapiValue::Binary(minimal_custom_action_roaming_xml_stream()))
                }
                PID_NAME_CONTENT_CLASS_W_TAG
                    if crate::mapi_store::is_outlook_configuration_message_class(
                        &message.message_class,
                    ) =>
                {
                    Some(MapiValue::String("urn:content-classes:message".to_string()))
                }
                PID_NAME_CONTENT_TYPE_W_TAG
                    if crate::mapi_store::is_outlook_configuration_message_class(
                        &message.message_class,
                    ) =>
                {
                    Some(MapiValue::String("text/xml".to_string()))
                }
                PID_LID_OUTLOOK_SHARING_PROVIDER_GUID_TAG
                    if is_outlook_virtual_sharing_state_config(message) =>
                {
                    Some(MapiValue::Guid(Uuid::nil().into_bytes()))
                }
                PID_LID_OUTLOOK_SHARING_REMOTE_NAME_TAG
                    if is_outlook_virtual_sharing_state_config(message) =>
                {
                    Some(MapiValue::String(String::new()))
                }
                PID_LID_OUTLOOK_SHARING_REMOTE_UID_TAG
                    if is_outlook_virtual_sharing_state_config(message) =>
                {
                    Some(MapiValue::String(String::new()))
                }
                PID_LID_OUTLOOK_SHARING_LOCAL_TYPE_TAG
                    if is_outlook_virtual_sharing_state_config(message) =>
                {
                    Some(MapiValue::Guid(Uuid::nil().into_bytes()))
                }
                PID_NAME_SHARING_SEND_AS_STATE_TAG | PID_LID_OUTLOOK_SHARING_8AA6_TAG
                    if is_outlook_virtual_sharing_state_config(message) =>
                {
                    Some(MapiValue::U32(0))
                }
                PID_LID_OUTLOOK_SHARING_CAPABILITIES_TAG
                    if is_outlook_virtual_sharing_state_config(message) =>
                {
                    Some(MapiValue::U32(0))
                }
                0x685D_0003
                    if crate::mapi_store::is_outlook_configuration_message_class(
                        &message.message_class,
                    ) =>
                {
                    Some(MapiValue::U32(outlook_configuration_stamp(message)))
                }
                PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
                _ => None,
            }
        })
}

fn is_umolk_computed_property(property_tag: u32) -> bool {
    matches!(
        property_tag,
        PID_TAG_MID
            | PID_TAG_INST_ID
            | PID_TAG_INSTANCE_NUM
            | PID_TAG_ENTRY_ID
            | PID_TAG_INSTANCE_KEY
            | PID_TAG_SUBJECT_W
            | PID_TAG_NORMALIZED_SUBJECT_W
            | PID_TAG_CONVERSATION_TOPIC_W
            | PID_TAG_MESSAGE_CLASS_W
            | PID_TAG_ORIGINAL_MESSAGE_CLASS_W
            | PID_TAG_MESSAGE_FLAGS
            | PID_TAG_MESSAGE_STATUS
            | PID_TAG_ACCESS_LEVEL
            | PID_TAG_ACCESS
            | PID_TAG_CLIENT_SUBMIT_TIME
            | PID_TAG_CREATION_TIME
            | PID_TAG_LAST_MODIFIER_NAME_W
            | PID_TAG_ASSOCIATED
            | PID_TAG_MESSAGE_SIZE
            | PID_TAG_MESSAGE_SIZE_EXTENDED
            | PID_TAG_FOLDER_ID
            | PID_TAG_PARENT_FOLDER_ID
            | PID_TAG_SOURCE_KEY
            | PID_TAG_RECORD_KEY
            | PID_TAG_SEARCH_KEY
            | PID_TAG_PARENT_SOURCE_KEY
            | PID_TAG_PARENT_ENTRY_ID
            | PID_TAG_CHANGE_KEY
            | PID_TAG_PREDECESSOR_CHANGE_LIST
            | PID_TAG_CHANGE_NUMBER
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_ROAMING_DATATYPES
            | PID_TAG_ROAMING_DICTIONARY
            | PID_NAME_CONTENT_CLASS_W_TAG
            | PID_NAME_CONTENT_TYPE_W_TAG
    )
}

fn associated_config_last_modified_filetime(message: &MapiAssociatedConfigMessage) -> Option<u64> {
    message
        .properties_json
        .get("__lpe_updated_at")
        .and_then(serde_json::Value::as_str)
        .map(mapi_mailstore::filetime_from_rfc3339_utc)
        .filter(|filetime| *filetime != 0)
}

fn associated_config_creation_filetime(message: &MapiAssociatedConfigMessage) -> Option<u64> {
    message
        .properties_json
        .get("__lpe_created_at")
        .and_then(serde_json::Value::as_str)
        .map(mapi_mailstore::filetime_from_rfc3339_utc)
        .filter(|filetime| *filetime != 0)
}

fn associated_config_last_modifier_name(message: &MapiAssociatedConfigMessage) -> Option<String> {
    message
        .properties_json
        .get("__lpe_last_modifier_name")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn is_outlook_virtual_sharing_state_config(message: &MapiAssociatedConfigMessage) -> bool {
    matches!(
        message.message_class.as_str(),
        "IPM.Aggregation" | "IPM.Sharing.Configuration" | "IPM.Sharing.Index"
    )
}

fn configuration_roaming_datatypes(
    message_class: &str,
    properties: &HashMap<u32, MapiValue>,
) -> u32 {
    let mut datatypes = 0;
    if properties.contains_key(&0x7C09_0102) {
        datatypes |= 0x0000_0001;
    }
    if properties.contains_key(&PID_TAG_ROAMING_XML_STREAM) {
        datatypes |= 0x0000_0002;
    }
    if properties.contains_key(&PID_TAG_ROAMING_DICTIONARY) {
        datatypes |= 0x0000_0004;
    }
    if datatypes == 0 {
        if configuration_uses_xml_stream(message_class) {
            0x0000_0002
        } else {
            0x0000_0004
        }
    } else {
        datatypes
    }
}

fn configuration_uses_xml_stream(message_class: &str) -> bool {
    crate::mapi_store::is_outlook_configuration_message_class_name(
        message_class,
        "IPM.Configuration.CategoryList",
    ) || crate::mapi_store::is_outlook_configuration_message_class_name(
        message_class,
        "IPM.Configuration.MRM",
    ) || crate::mapi_store::is_outlook_configuration_message_class_name(
        message_class,
        "IPM.Configuration.WorkHours",
    )
}

pub(in crate::mapi) fn minimal_roaming_dictionary_stream() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="utf-8"?><UserConfiguration xmlns="dictionary.xsd"><Info version="Outlook.16"/><Data><e k="18-OLPrefsVersion" v="9-1"/></Data></UserConfiguration>"#.to_vec()
}

fn minimal_custom_action_roaming_xml_stream() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="utf-8"?><customActions xmlns="http://schemas.microsoft.com/office/outlook/quicksteps/2010" version="1"/>"#.to_vec()
}

fn minimal_working_hours_roaming_xml_stream() -> Vec<u8> {
    br#"<?xml version="1.0"?><Root xmlns="WorkingHours.xsd"><WorkHoursVersion1><TimeZone><Bias>0</Bias><Standard><Bias>0</Bias><ChangeDate><Time>02:00:00</Time><Date>0000/11/01</Date><DayOfWeek>0</DayOfWeek></ChangeDate></Standard><DaylightSavings><Bias>0</Bias><ChangeDate><Time>02:00:00</Time><Date>0000/03/02</Date><DayOfWeek>0</DayOfWeek></ChangeDate></DaylightSavings><Name>UTC</Name></TimeZone><TimeSlot><Start>09:00:00</Start><End>17:00:00</End></TimeSlot><WorkDays>Monday Tuesday Wednesday Thursday Friday</WorkDays></WorkHoursVersion1></Root>"#.to_vec()
}

fn minimal_category_list_roaming_xml_stream() -> Vec<u8> {
    br#"<?xml version="1.0"?><categories default="Red Category" lastSavedSession="0" lastSavedTime="1601-01-01T00:00:00.000" xmlns="CategoryList.xsd"><category name="Red Category" color="0" keyboardShortcut="0" usageCount="0" lastTimeUsedNotes="1601-01-01T00:00:00.000" lastTimeUsedJournal="1601-01-01T00:00:00.000" lastTimeUsedContacts="1601-01-01T00:00:00.000" lastTimeUsedTasks="1601-01-01T00:00:00.000" lastTimeUsedCalendar="1601-01-01T00:00:00.000" lastTimeUsedMail="1601-01-01T00:00:00.000" lastTimeUsed="1601-01-01T00:00:00.000" lastSessionUsed="0" guid="{2B7FC69C-7046-44A2-8FF3-007D7467DC82}"/></categories>"#.to_vec()
}

fn minimal_mrm_roaming_xml_stream() -> Vec<u8> {
    br#"<?xml version="1.0"?><UserConfiguration><Info version="LPE.1"><Data><RetentionHold Enabled="False" RetentionComment="" RetentionUrl=""/></Data></Info></UserConfiguration>"#.to_vec()
}

fn outlook_configuration_stamp(message: &MapiAssociatedConfigMessage) -> u32 {
    let mut hash = 0x811c_9dc5u32;
    for byte in message
        .message_class
        .as_bytes()
        .iter()
        .chain(message.subject.as_bytes())
    {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash.max(1)
}

fn delegate_freebusy_message_size(message: &MapiDelegateFreeBusyMessage) -> i64 {
    message
        .message
        .subject
        .len()
        .saturating_add(message.message.body_text.len())
        .saturating_add(message.message.payload_json.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn delegate_freebusy_property_value(
    message: &MapiDelegateFreeBusyMessage,
    property_tag: u32,
) -> Option<MapiValue> {
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);
    match canonical_property_storage_tag(property_tag) {
        PID_TAG_MID => Some(MapiValue::U64(message.id)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(message.id),
        )),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(message.message.subject.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(message.message.body_text.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            if message.message.message_kind == "delegate" {
                "IPM.Microsoft.Delegate".to_string()
            } else {
                "IPM.Microsoft.ScheduleData.FreeBusy".to_string()
            },
        )),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0x0000_0040)),
        PID_TAG_ASSOCIATED => Some(MapiValue::Bool(true)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(delegate_freebusy_message_size(
            message,
        ))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(
            delegate_freebusy_message_size(message),
        )),
        PID_TAG_PARENT_FOLDER_ID => Some(MapiValue::U64(message.folder_id)),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            message.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(message.folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME
        | PID_TAG_MESSAGE_DELIVERY_TIME => Some(MapiValue::I64(
            mapi_mailstore::filetime_from_rfc3339_utc(&message.message.updated_at) as i64,
        )),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE => Some(MapiValue::U32(0)),
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B => Some(MapiValue::Binary(Vec::new())),
        0x6842_000B | 0x6843_000B | 0x684B_000B | 0x686D_000B | 0x686E_000B | 0x686F_000B => {
            Some(MapiValue::Bool(false))
        }
        0x6844_101F | 0x684A_101F => Some(MapiValue::MultiString(Vec::new())),
        0x6845_1102 | 0x6870_1102 => Some(MapiValue::MultiBinary(Vec::new())),
        0x686B_1003 | 0x6871_1003 => Some(MapiValue::MultiI32(Vec::new())),
        0x6872_001F => Some(MapiValue::String(String::new())),
        _ => None,
    }
}
