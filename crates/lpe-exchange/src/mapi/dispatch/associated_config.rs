use super::*;

pub(super) async fn delete_associated_config_properties<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    config_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    saved_message: Option<&crate::mapi_store::MapiAssociatedConfigMessage>,
    property_tags: &[u32],
) -> Result<(usize, crate::mapi_store::MapiAssociatedConfigMessage)>
where
    S: ExchangeStore,
{
    if property_tags.is_empty() {
        let existing =
            associated_config_message_for_mutation(snapshot, folder_id, config_id, saved_message)
                .ok_or_else(|| anyhow!("MAPI associated config message was not found"))?;
        return Ok((0, existing));
    }
    let Some(existing) =
        associated_config_message_for_mutation(snapshot, folder_id, config_id, saved_message)
    else {
        return Err(anyhow!("MAPI associated config message was not found"));
    };
    if existing.folder_id != folder_id {
        return Err(anyhow!("MAPI associated config message was not found"));
    }
    let mut properties = associated_config_mutation_base_properties(&existing);
    let mut deleted = 0usize;
    for tag in property_tags
        .iter()
        .flat_map(|tag| [*tag, canonical_property_storage_tag(*tag)])
    {
        if properties.remove(&tag).is_some() {
            deleted += 1;
        }
    }
    let (message_class, subject) = associated_config_class_and_subject(&properties);
    let properties = normalized_associated_config_persisted_properties(&message_class, &properties);
    let saved = store
        .upsert_mapi_associated_config(UpsertMapiAssociatedConfigInput {
            id: Some(existing.canonical_id),
            account_id: principal.account_id,
            folder_id,
            message_class,
            subject,
            properties_json: mapi_properties_to_json(&properties),
        })
        .await?;
    Ok((
        deleted,
        crate::mapi_store::MapiAssociatedConfigMessage {
            id: config_id,
            folder_id,
            canonical_id: saved.id,
            message_class: saved.message_class,
            subject: saved.subject,
            properties_json: saved.properties_json,
        },
    ))
}

pub(super) fn associated_config_message_for_mutation(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    config_id: u64,
    saved_message: Option<&crate::mapi_store::MapiAssociatedConfigMessage>,
) -> Option<crate::mapi_store::MapiAssociatedConfigMessage> {
    // MS-OXOCFG 3.1.4.2 and MS-OXCROPS 2.2.8.6, 2.2.8.9, and 2.2.6.3:
    // mutations on one open configuration message are cumulative until save.
    saved_message
        .cloned()
        .or_else(|| snapshot.associated_config_message_for_id(config_id))
        .filter(|message| message.folder_id == folder_id)
}

pub(super) async fn set_associated_config_properties<S>(
    store: &S,
    principal: &AccountPrincipal,
    existing: &crate::mapi_store::MapiAssociatedConfigMessage,
    values: Vec<(u32, MapiValue)>,
) -> Result<crate::mapi_store::MapiAssociatedConfigMessage>
where
    S: ExchangeStore,
{
    let mut properties = associated_config_mutation_base_properties(existing);
    apply_mapi_property_values_to_map(&mut properties, values);
    let (message_class, subject) = associated_config_class_and_subject(&properties);
    let properties = normalized_associated_config_persisted_properties(&message_class, &properties);
    let saved = store
        .upsert_mapi_associated_config(UpsertMapiAssociatedConfigInput {
            id: Some(existing.canonical_id),
            account_id: principal.account_id,
            folder_id: existing.folder_id,
            message_class,
            subject,
            properties_json: mapi_properties_to_json(&properties),
        })
        .await?;
    Ok(crate::mapi_store::MapiAssociatedConfigMessage {
        id: existing.id,
        folder_id: existing.folder_id,
        canonical_id: saved.id,
        message_class: saved.message_class,
        subject: saved.subject,
        properties_json: saved.properties_json,
    })
}

pub(super) fn delegate_freebusy_message_for_open<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<&'a crate::mapi_store::MapiDelegateFreeBusyMessage> {
    (folder_id == FREEBUSY_DATA_FOLDER_ID)
        .then(|| snapshot.delegate_freebusy_message_for_id(message_id))
        .flatten()
        .filter(|message| message.folder_id == folder_id)
}

pub(super) fn conversation_action_message_for_open(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<crate::mapi_store::MapiConversationActionMessage> {
    (folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID)
        .then(|| snapshot.conversation_action_table_message_for_id(message_id))
        .flatten()
        .filter(|message| message.folder_id == folder_id)
}

pub(super) fn navigation_shortcut_message_for_open(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<crate::mapi_store::MapiNavigationShortcutMessage> {
    (folder_id == COMMON_VIEWS_FOLDER_ID)
        .then(|| snapshot.navigation_shortcut_table_message_for_id(message_id))
        .flatten()
        .filter(|message| message.folder_id == folder_id)
}

pub(super) fn common_view_named_view_message_for_open(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<crate::mapi_store::MapiCommonViewNamedViewMessage> {
    if folder_id == COMMON_VIEWS_FOLDER_ID {
        return snapshot
            .common_view_named_view_message_for_id(message_id)
            .filter(|message| message.folder_id == folder_id);
    }
    folder_local_default_named_view_is_supported(snapshot, folder_id, message_id)
        .then(|| snapshot.default_folder_named_view_message(folder_id, message_id))
        .flatten()
}

pub(super) fn search_folder_definition_message_for_open(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> Option<SearchFolderDefinition> {
    (folder_id == COMMON_VIEWS_FOLDER_ID)
        .then(|| {
            snapshot.common_views_table_messages().find_map(|message| {
                if let crate::mapi_store::MapiCommonViewsMessage::SearchFolderDefinition(
                    definition,
                ) = message
                {
                    (crate::mapi::identity::mapped_mapi_object_id(&definition.id)
                        == Some(message_id))
                    .then_some(definition)
                } else {
                    None
                }
            })
        })
        .flatten()
}

pub(super) fn associated_config_mutation_base_properties(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> HashMap<u32, MapiValue> {
    let mut properties = mapi_properties_from_json(&message.properties_json);
    for tag in [
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_ORIGINAL_MESSAGE_CLASS_W,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
    ] {
        properties.entry(tag).or_insert_with(|| {
            associated_config_property_value(message, tag)
                .expect("associated config identity property should be available")
        });
    }
    properties
}

pub(super) async fn persist_associated_config_message<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    properties: &HashMap<u32, MapiValue>,
) -> Result<(crate::store::MapiAssociatedConfigRecord, u64)>
where
    S: ExchangeStore,
{
    let source_key = imported_message_source_key(properties);
    let reserved_global_counter = source_key
        .as_deref()
        .and_then(persistable_import_source_key_global_counter);
    let (message_class, subject) = associated_config_class_and_subject(properties);
    let properties = normalized_associated_config_persisted_properties(&message_class, properties);
    let id = associated_config_uuid(&properties);
    if is_empty_inbox_message_list_settings_placeholder(folder_id, &message_class, &properties) {
        let default = crate::mapi_store::outlook_inbox_message_list_settings_default();
        let reserved_global_counter =
            crate::mapi::identity::global_counter_from_store_id(default.id);
        let persisted_properties = message_list_settings_placeholder_persisted_properties(&default);
        let saved = store
            .upsert_mapi_associated_config(UpsertMapiAssociatedConfigInput {
                id: Some(default.canonical_id),
                account_id: principal.account_id,
                folder_id: default.folder_id,
                message_class: default.message_class.clone(),
                subject: default.subject.clone(),
                properties_json: mapi_properties_to_json(&persisted_properties),
            })
            .await?;
        let message_id = remember_created_mapi_identity(
            store,
            principal,
            MapiIdentityObjectKind::AssociatedConfig,
            default.canonical_id,
            reserved_global_counter,
            None,
        )
        .await?;
        return Ok((
            crate::store::MapiAssociatedConfigRecord {
                id: default.canonical_id,
                account_id: principal.account_id,
                folder_id: default.folder_id,
                message_class: saved.message_class,
                subject: saved.subject,
                properties_json: saved.properties_json,
            },
            message_id,
        ));
    }
    let saved = store
        .upsert_mapi_associated_config(UpsertMapiAssociatedConfigInput {
            id: Some(id),
            account_id: principal.account_id,
            folder_id,
            message_class,
            subject,
            properties_json: mapi_properties_to_json(&properties),
        })
        .await?;
    let message_id = remember_created_mapi_identity(
        store,
        principal,
        MapiIdentityObjectKind::AssociatedConfig,
        saved.id,
        reserved_global_counter,
        source_key.filter(|_| reserved_global_counter.is_some()),
    )
    .await?;
    Ok((saved, message_id))
}

pub(super) async fn persist_associated_config_stream_message<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> Result<()>
where
    S: ExchangeStore,
{
    let properties = normalized_associated_config_persisted_properties(
        &message.message_class,
        &mapi_properties_from_json(&message.properties_json),
    );
    store
        .upsert_mapi_associated_config(UpsertMapiAssociatedConfigInput {
            id: Some(message.canonical_id),
            account_id: principal.account_id,
            folder_id,
            message_class: message.message_class.clone(),
            subject: message.subject.clone(),
            properties_json: mapi_properties_to_json(&properties),
        })
        .await?;
    Ok(())
}

pub(super) fn normalized_associated_config_persisted_properties(
    message_class: &str,
    properties: &HashMap<u32, MapiValue>,
) -> HashMap<u32, MapiValue> {
    let mut normalized = properties.clone();
    if !message_class
        .get(..18)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("IPM.Configuration."))
    {
        return normalized;
    }
    if matches!(
        normalized.get(&PID_TAG_ROAMING_DICTIONARY),
        Some(MapiValue::Binary(value))
            if value.as_slice() == b"<xml/>"
                || crate::mapi::tables::is_stale_minimal_umolk_dictionary(
                    message_class,
                    value,
                )
    ) {
        normalized.insert(
            PID_TAG_ROAMING_DICTIONARY,
            MapiValue::Binary(minimal_roaming_dictionary_stream()),
        );
    }
    normalized
}

pub(super) async fn persist_released_associated_config_stream<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    released_object: Option<&MapiObject>,
) -> Result<()>
where
    S: ExchangeStore,
{
    let Some(MapiObject::AttachmentStream {
        writable_target: Some(StreamWriteTarget::AssociatedConfigProperty { handle, .. }),
        ..
    }) = released_object
    else {
        return Ok(());
    };
    let message = match session.handles.get(handle) {
        Some(MapiObject::AssociatedConfig {
            folder_id,
            saved_message: Some(message),
            ..
        }) => Some((*folder_id, message.clone())),
        _ => None,
    };
    match message {
        Some((folder_id, message)) => {
            persist_associated_config_stream_message(store, principal, folder_id, &message).await
        }
        None => Err(anyhow!(
            "MAPI associated config stream release target was not found"
        )),
    }
}

pub(super) fn message_list_settings_placeholder_persisted_properties(
    default: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> HashMap<u32, MapiValue> {
    HashMap::from([
        (
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String(default.message_class.clone()),
        ),
        (
            PID_TAG_ORIGINAL_MESSAGE_CLASS_W,
            MapiValue::String(default.message_class.clone()),
        ),
        (
            PID_TAG_SUBJECT_W,
            MapiValue::String(default.subject.clone()),
        ),
        (
            PID_TAG_NORMALIZED_SUBJECT_W,
            MapiValue::String(default.subject.clone()),
        ),
        (PID_TAG_ROAMING_DATATYPES, MapiValue::U32(0x0000_0004)),
        (
            PID_TAG_ROAMING_DICTIONARY,
            MapiValue::Binary(minimal_roaming_dictionary_stream()),
        ),
    ])
}

pub(super) fn is_empty_inbox_message_list_settings_placeholder(
    folder_id: u64,
    message_class: &str,
    properties: &HashMap<u32, MapiValue>,
) -> bool {
    folder_id == INBOX_FOLDER_ID
        && crate::mapi_store::is_outlook_configuration_message_class_name(
            message_class,
            "IPM.Configuration.MessageListSettings",
        )
        && properties
            .get(&PID_TAG_ROAMING_DATATYPES)
            .cloned()
            .and_then(MapiValue::into_u32)
            .unwrap_or(0)
            == 0
        && !properties.contains_key(&PID_TAG_ROAMING_DICTIONARY)
        && !properties.contains_key(&PID_TAG_ROAMING_XML_STREAM)
        && !properties.contains_key(&OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B)
        && !properties.contains_key(&0x7C09_0102)
}

pub(super) fn associated_config_uuid(properties: &HashMap<u32, MapiValue>) -> Uuid {
    let mut hasher = Sha256::new();
    if let Some(source_key) = imported_message_source_key(properties) {
        hasher.update(source_key);
    } else {
        let mut tags = properties.keys().copied().collect::<Vec<_>>();
        tags.sort_unstable();
        for tag in tags {
            hasher.update(tag.to_le_bytes());
            if let Some(value) = properties.get(&tag) {
                hasher.update(format!("{value:?}").as_bytes());
            }
        }
    }
    let digest = hasher.finalize();
    Uuid::from_bytes(digest[..16].try_into().expect("sha256 digest prefix"))
}

pub(super) fn associated_config_class_and_subject(
    properties: &HashMap<u32, MapiValue>,
) -> (String, String) {
    let message_class = properties
        .get(&PID_TAG_MESSAGE_CLASS_W)
        .or_else(|| properties.get(&0x001A_001E))
        .cloned()
        .and_then(MapiValue::into_text)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "IPM.Configuration".to_string());
    let subject = properties
        .get(&PID_TAG_SUBJECT_W)
        .or_else(|| properties.get(&PID_TAG_NORMALIZED_SUBJECT_W))
        .cloned()
        .and_then(MapiValue::into_text)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| message_class.clone());
    (message_class, subject)
}

pub(super) fn transient_associated_message_id(
    folder_id: u64,
    properties: &HashMap<u32, MapiValue>,
) -> u64 {
    imported_message_source_key(properties)
        .as_deref()
        .and_then(source_key_global_counter)
        .map(crate::mapi::identity::mapi_store_id)
        .unwrap_or_else(|| {
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER.saturating_add(
                    crate::mapi::identity::global_counter_from_store_id(folder_id).unwrap_or(1),
                ),
            )
        })
}

pub(super) fn transient_client_local_message_id(message_id: u64) -> bool {
    crate::mapi::identity::global_counter_from_store_id(message_id)
        .is_some_and(|counter| counter > crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER)
}
