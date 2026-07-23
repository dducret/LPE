use super::*;

pub(super) fn associated_config_sync_object(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut named_properties = Vec::new();
    let stored_properties = mapi_properties_from_json(&message.properties_json);
    for (tag, value) in stored_properties.clone() {
        if associated_config_standard_sync_tag(tag)
            || crate::mapi_store::is_associated_config_read_only_property_tag(tag)
        {
            continue;
        }
        if let Some(value) = special_message_property_value(value) {
            named_properties.push((tag, value));
        }
    }
    for tag in [PID_TAG_CREATION_TIME, PID_TAG_LAST_MODIFIER_NAME_W] {
        if let Some(value) =
            associated_config_property_value(message, tag).and_then(special_message_property_value)
        {
            named_properties.push((tag, value));
        }
    }
    // Keep the LPE projection stable: [MS-OXCFXICS] section 2.2.4.2
    // serializes propList in the supplied sequence, while the persisted bag
    // is rehydrated as a HashMap before the same FAI version is projected.
    named_properties.sort_unstable_by_key(|(tag, _)| *tag);
    for &tag in associated_config_default_sync_tags(message, &stored_properties) {
        let canonical_tag = canonical_property_storage_tag(tag);
        if associated_config_standard_sync_tag(canonical_tag)
            || stored_properties.contains_key(&canonical_tag)
        {
            continue;
        }
        if let Some(value) =
            associated_config_property_value(message, tag).and_then(special_message_property_value)
        {
            named_properties.push((tag, value));
        }
    }
    let change_number = stored_properties
        .get(&PID_TAG_CHANGE_NUMBER)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_else(|| mapi_mailstore::change_number_for_store_id(message.id));
    let last_modified_filetime = stored_properties
        .get(&PID_TAG_LAST_MODIFICATION_TIME)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_else(|| mapi_mailstore::filetime_from_change_number(change_number));
    let message_size = message
        .subject
        .len()
        .saturating_add(message.message_class.len())
        .saturating_add(message.properties_json.to_string().len())
        .min(i64::MAX as usize) as i64;

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: message.subject.clone(),
        body_text: associated_config_text_property(message, PID_TAG_BODY_W),
        message_class: message.message_class.clone(),
        last_modified_filetime,
        message_size,
        read_state: None,
        named_properties,
        named_property_definitions: HashMap::new(),
    }
}

fn associated_config_default_sync_tags(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
    stored_properties: &HashMap<u32, MapiValue>,
) -> &'static [u32] {
    if crate::mapi_store::is_outlook_configuration_message_class(&message.message_class) {
        // [MS-OXOCFG] sections 2.2.2.1 and 2.2.5.1: a persisted
        // PidTagRoamingDatatypes value is the client's complete declaration of
        // the streams that exist. LPE therefore preserves the client-owned bag
        // in CopyTo/ICS instead of adding absent compatibility properties.
        if stored_properties.contains_key(&PID_TAG_ROAMING_DATATYPES) {
            return &[];
        }
        &[
            PID_TAG_ROAMING_DATATYPES,
            PID_TAG_ROAMING_DICTIONARY,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
            PID_NAME_CONTENT_CLASS_W_TAG,
            PID_NAME_CONTENT_TYPE_W_TAG,
        ]
    } else {
        &[]
    }
}

fn associated_config_standard_sync_tag(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_FOLDER_ID
            | PID_TAG_MID
            | PID_TAG_INST_ID
            | PID_TAG_INSTANCE_NUM
            | PID_TAG_ENTRY_ID
            | PID_TAG_INSTANCE_KEY
            | PID_TAG_ASSOCIATED
            | PID_TAG_MESSAGE_SIZE
            | PID_TAG_SUBJECT_W
            | PID_TAG_NORMALIZED_SUBJECT_W
            | PID_TAG_MESSAGE_CLASS_W
            | PID_TAG_BODY_W
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_ACCESS
            | PID_TAG_ACCESS_LEVEL
    )
}

fn associated_config_text_property(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
    tag: u32,
) -> Option<String> {
    mapi_properties_from_json(&message.properties_json)
        .remove(&tag)
        .and_then(MapiValue::into_text)
}
