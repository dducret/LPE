use super::*;

pub(super) fn delete_associated_config_properties(
    folder_id: u64,
    config_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    saved_message: Option<&crate::mapi_store::MapiAssociatedConfigMessage>,
    property_tags: &[u32],
) -> Result<(usize, crate::mapi_store::MapiAssociatedConfigMessage)> {
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
    let mut properties_json = mapi_properties_to_json(&properties);
    crate::mapi_store::copy_associated_config_server_metadata(
        &existing.properties_json,
        &mut properties_json,
    );
    Ok((
        deleted,
        crate::mapi_store::MapiAssociatedConfigMessage {
            id: config_id,
            folder_id,
            canonical_id: existing.canonical_id,
            message_class,
            subject,
            properties_json,
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

pub(super) fn set_associated_config_properties(
    existing: &crate::mapi_store::MapiAssociatedConfigMessage,
    values: Vec<(u32, MapiValue)>,
) -> Result<crate::mapi_store::MapiAssociatedConfigMessage> {
    let mut properties = associated_config_mutation_base_properties(existing);
    // [MS-OXCPRPT] sections 2.2.1.4, 2.2.1.5, and 3.2.5.4: Outlook can
    // submit these properties during an ICS FAI upload, but the server must
    // not retain client-selected CreationTime or LastModifierName values.
    let values = values
        .into_iter()
        .filter(|(tag, _)| !crate::mapi_store::is_associated_config_read_only_property_tag(*tag))
        .collect();
    apply_mapi_property_values_to_map(&mut properties, values);
    let (message_class, subject) = associated_config_class_and_subject(&properties);
    let mut properties_json = mapi_properties_to_json(&properties);
    crate::mapi_store::copy_associated_config_server_metadata(
        &existing.properties_json,
        &mut properties_json,
    );
    Ok(crate::mapi_store::MapiAssociatedConfigMessage {
        id: existing.id,
        folder_id: existing.folder_id,
        canonical_id: existing.canonical_id,
        message_class,
        subject,
        properties_json,
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
    imported_message_id: Option<u64>,
    fail_on_conflict: bool,
) -> Result<(
    crate::store::MapiAssociatedConfigRecord,
    crate::store::MapiIdentityRecord,
    Option<MapiFaiImportDisposition>,
)>
where
    S: ExchangeStore,
{
    let imported_identity = imported_message_id
        .map(|message_id| imported_fai_identity(properties, message_id))
        .transpose()?;
    let (message_class, subject) = associated_config_class_and_subject(properties);
    let imported_canonical_id = imported_identity
        .as_ref()
        .map(|_| associated_config_uuid(properties));
    let normalized = normalized_associated_config_persisted_properties(&message_class, properties);
    // [MS-OXCPRPT] section 3.2.5.4, [MS-OXCMSG] section 3.2.5.3, and
    // [MS-OXOCFG] sections 2.2.2.1 and 2.2.5.1: a successful Save commits
    // the client's configuration-property values. In particular, an explicit
    // zero PidTagRoamingDatatypes value declares that no roaming stream exists.
    let mut input = UpsertMapiAssociatedConfigInput {
        // [MS-OXCMSG] sections 2.2.3.2 and 2.2.3.3: each newly created Message
        // has its own identity even when its payload equals another FAI message.
        id: Some(imported_canonical_id.unwrap_or_else(Uuid::new_v4)),
        account_id: principal.account_id,
        folder_id,
        message_class,
        subject,
        properties_json: mapi_properties_to_json(&normalized),
    };

    // mapi_object_identities is the sole durable ICS identity. Content JSON
    // contains only user/configuration properties and receives the identity
    // tuple as an ephemeral snapshot projection on reload.
    let mut content_properties = mapi_properties_from_json(&input.properties_json);
    remove_associated_config_server_owned_properties(&mut content_properties);
    input.properties_json = mapi_properties_to_json(&content_properties);

    if let Some(imported_identity) = imported_identity {
        let committed = store
            .commit_mapi_associated_config_import(CommitMapiAssociatedConfigImportInput {
                config: input,
                identity: imported_identity,
                fail_on_conflict,
            })
            .await?;
        crate::mapi::identity::remember_mapi_identity_with_source_key(
            committed.config.id,
            committed.identity.object_id,
            Some(committed.identity.source_key.clone()),
        );
        return Ok((
            committed.config,
            committed.identity,
            Some(committed.disposition),
        ));
    }

    let committed = store.commit_mapi_associated_config_create(input).await?;
    crate::mapi::identity::remember_mapi_identity_with_source_key(
        committed.config.id,
        committed.identity.object_id,
        Some(committed.identity.source_key.clone()),
    );
    Ok((committed.config, committed.identity, None))
}

fn remove_associated_config_server_owned_properties(properties: &mut HashMap<u32, MapiValue>) {
    properties
        .retain(|tag, _| !crate::mapi_store::is_associated_config_server_owned_property_tag(*tag));
}

fn associated_config_message_with_identity(
    saved: &crate::store::MapiAssociatedConfigRecord,
    identity: &crate::store::MapiIdentityRecord,
) -> crate::mapi_store::MapiAssociatedConfigMessage {
    let mut properties = mapi_properties_from_json(&saved.properties_json);
    properties.insert(PID_TAG_MID, MapiValue::U64(identity.object_id));
    properties.insert(
        PID_TAG_SOURCE_KEY,
        MapiValue::Binary(identity.source_key.clone()),
    );
    properties.insert(
        PID_TAG_CHANGE_KEY,
        MapiValue::Binary(identity.change_key.clone()),
    );
    properties.insert(
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        MapiValue::Binary(identity.predecessor_change_list.clone()),
    );
    properties.insert(
        PID_TAG_CHANGE_NUMBER,
        MapiValue::U64(identity.change_number),
    );
    properties.insert(
        PID_TAG_LAST_MODIFICATION_TIME,
        MapiValue::I64(identity.last_modification_time as i64),
    );
    let mut properties_json = mapi_properties_to_json(&properties);
    crate::mapi_store::copy_associated_config_server_metadata(
        &saved.properties_json,
        &mut properties_json,
    );
    crate::mapi_store::MapiAssociatedConfigMessage {
        id: identity.object_id,
        folder_id: saved.folder_id,
        canonical_id: saved.id,
        message_class: saved.message_class.clone(),
        subject: saved.subject.clone(),
        properties_json,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_pending_associated_config_save_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    mapi_request_id: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    responses: &mut Vec<u8>,
    handle: u32,
    folder_id: u64,
    properties: &HashMap<u32, MapiValue>,
    imported_message_id: Option<u64>,
    fail_on_conflict: bool,
) {
    match persist_associated_config_message(
        store,
        principal,
        folder_id,
        properties,
        imported_message_id,
        fail_on_conflict,
    )
    .await
    {
        Ok((saved, identity, import_disposition)) => {
            let message_id = identity.object_id;
            let changed = import_disposition
                .map(MapiFaiImportDisposition::changes_server_replica)
                .unwrap_or(true);
            session.record_last_post_hierarchy_create_save_object_context(format!(
                "kind=associated_config;send_candidate=false;create_associated=true;request_id={mapi_request_id};folder=0x{folder_id:016x};role={};class={};subject={};mapi_message_id=0x{message_id:016x};canonical_id={};property_count={}",
                debug_role_for_folder_id(folder_id),
                saved.message_class,
                saved.subject,
                saved.id,
                saved.properties_json.as_object().map_or(0, |properties| properties.len())
            ));
            session.handles.insert(
                handle,
                MapiObject::AssociatedConfig {
                    folder_id,
                    config_id: message_id,
                    saved_message: Some(associated_config_message_with_identity(&saved, &identity)),
                },
            );
            if import_disposition
                .map(fai_import_is_reflected_in_client_replica)
                .unwrap_or(true)
            {
                record_sync_upload_content_change(
                    session,
                    folder_id,
                    message_id,
                    identity.change_number,
                    true,
                    false,
                );
            }
            if changed {
                session.record_notification(MapiNotificationEvent::content(
                    folder_id,
                    Some(message_id),
                ));
            }
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                request,
                handle,
                message_id,
            );
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = %mapi_request_id,
                request_rop_id = "0x0c",
                folder_id = %format!("{folder_id:#018x}"),
                associated_config_id = %saved.id,
                mapi_message_id = %format!("{message_id:#018x}"),
                associated_message_class = %saved.message_class,
                associated_subject = %saved.subject,
                property_count = saved.properties_json.as_object().map_or(0, |properties| properties.len()),
                "rca debug persisted associated config message"
            );
        }
        Err(error) => {
            let return_value = if error.is::<crate::store::MapiFaiImportObjectDeleted>() {
                0x8004_010A
            } else if error.is::<crate::store::MapiFaiImportConflict>() {
                0x8004_0109
            } else {
                0x8004_010F
            };
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                return_value,
            ));
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_existing_associated_config_save_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    handle: u32,
    folder_id: u64,
    config_id: u64,
    saved_message: Option<&crate::mapi_store::MapiAssociatedConfigMessage>,
) {
    let Some(message) =
        associated_config_message_for_mutation(snapshot, folder_id, config_id, saved_message)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let staged_properties = associated_config_mutation_base_properties(&message);
    let (message_class, subject) = associated_config_class_and_subject(&staged_properties);
    let properties =
        normalized_associated_config_content_properties(&message_class, &staged_properties);
    let Some(current) =
        associated_config_message_for_mutation(snapshot, folder_id, config_id, None)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let current_properties = associated_config_mutation_base_properties(&current);
    let (current_message_class, current_subject) =
        associated_config_class_and_subject(&current_properties);
    let current_properties = normalized_associated_config_content_properties(
        &current_message_class,
        &current_properties,
    );
    if message.canonical_id == current.canonical_id
        && message_class == current_message_class
        && subject == current_subject
        && properties == current_properties
    {
        append_save_changes_message_response(
            session,
            responses,
            handle_slots,
            request,
            handle,
            config_id,
        );
        return;
    }
    match store
        .commit_mapi_associated_config_update(UpsertMapiAssociatedConfigInput {
            id: Some(message.canonical_id),
            account_id: principal.account_id,
            folder_id,
            message_class,
            subject,
            properties_json: mapi_properties_to_json(&properties),
        })
        .await
    {
        Ok(committed) => {
            let message_id = committed.identity.object_id;
            session.handles.insert(
                handle,
                MapiObject::AssociatedConfig {
                    folder_id,
                    config_id: message_id,
                    saved_message: Some(associated_config_message_with_identity(
                        &committed.config,
                        &committed.identity,
                    )),
                },
            );
            record_sync_upload_content_change(
                session,
                folder_id,
                message_id,
                committed.identity.change_number,
                true,
                false,
            );
            session
                .record_notification(MapiNotificationEvent::content(folder_id, Some(message_id)));
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                request,
                handle,
                message_id,
            );
        }
        Err(error) => {
            tracing::warn!(
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x0c",
                folder_id = %format!("{folder_id:#018x}"),
                config_id = %format!("{config_id:#018x}"),
                error = %error,
                "mapi associated config save failed"
            );
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_010F,
            ));
        }
    }
}

pub(super) fn normalized_associated_config_persisted_properties(
    message_class: &str,
    properties: &HashMap<u32, MapiValue>,
) -> HashMap<u32, MapiValue> {
    let mut normalized = properties.clone();
    // [MS-OXCMSG] section 2.2.1.6: the server sets or clears mfEverRead
    // whenever mfRead is set or cleared.
    if let Some(message_flags) = normalized.get_mut(&PID_TAG_MESSAGE_FLAGS) {
        let normalize_read_state = |flags: u32| {
            if flags & MSGFLAG_READ != 0 {
                flags | MSGFLAG_EVERREAD
            } else {
                flags & !MSGFLAG_EVERREAD
            }
        };
        match message_flags {
            MapiValue::I32(flags) => *flags = normalize_read_state(*flags as u32) as i32,
            MapiValue::U32(flags) => *flags = normalize_read_state(*flags),
            _ => {}
        }
    }
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

pub(super) fn normalized_associated_config_content_properties(
    message_class: &str,
    properties: &HashMap<u32, MapiValue>,
) -> HashMap<u32, MapiValue> {
    let mut normalized =
        normalized_associated_config_persisted_properties(message_class, properties);
    // Server-owned identity, version, and history properties are projected
    // from canonical metadata. Never re-persist that projection when an
    // existing FAI is mutated.
    remove_associated_config_server_owned_properties(&mut normalized);
    normalized
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
