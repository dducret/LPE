const PUBLIC_FOLDER_PER_USER_STREAM_MAGIC: &[u8; 8] = b"LPEPFU1\0";

use super::*;
use crate::store::MapiIdentityRequest;

pub(super) async fn hard_delete_public_folder_contents<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<(Vec<u64>, bool), u32> {
    let Some(folder) = snapshot.public_folder_for_id(folder_id) else {
        return Err(0x8004_010F);
    };
    let item_ids = snapshot
        .public_folder_items_for_folder(folder_id)
        .into_iter()
        .map(|item| item.item.id)
        .collect::<Vec<_>>();
    let attempted_count = item_ids.len();
    let mut succeeded_count = 0usize;
    let mut failed_count = 0usize;
    let mut partial_completion = false;

    for item_id in item_ids {
        if store
            .delete_public_folder_item(
                principal.account_id,
                folder.folder.id,
                item_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-hard-delete-public-folder-contents".to_string(),
                    subject: format!("public-folder:{} item:{}", folder.folder.id, item_id),
                },
            )
            .await
            .is_err()
        {
            partial_completion = true;
            failed_count += 1;
        } else {
            succeeded_count += 1;
        }
    }

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        mailbox = %principal.email,
        folder_id = %format!("{folder_id:#018x}"),
        public_folder_id = %folder.folder.id,
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
        message = "rca debug mapi hard delete public folder contents"
    );
    record_mapi_folder_purge_metrics(
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
    );
    Ok(if succeeded_count == 0 {
        (Vec::new(), partial_completion)
    } else {
        (vec![folder_id], partial_completion)
    })
}

pub(super) async fn copy_public_folder_tree_for_mapi<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    source_id: Uuid,
    target_parent_id: Uuid,
    root_display_name: &str,
) -> Result<lpe_storage::PublicFolder> {
    let mut stack = vec![(
        source_id,
        target_parent_id,
        Some(root_display_name.to_string()),
    )];
    let mut root = None;
    while let Some((current_id, parent_id, display_name_override)) = stack.pop() {
        let current = store
            .fetch_public_folder(principal.account_id, current_id)
            .await?;
        let copied = store
            .create_public_folder_child(
                CreatePublicFolderInput {
                    account_id: principal.account_id,
                    parent_folder_id: parent_id,
                    display_name: display_name_override
                        .unwrap_or_else(|| current.display_name.clone()),
                    folder_class: current.folder_class.clone(),
                    sort_order: current.sort_order,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-copy-public-folder".to_string(),
                    subject: current_id.to_string(),
                },
            )
            .await?;
        if current_id == source_id {
            root = Some(copied.clone());
        }
        for item in store
            .fetch_public_folder_items(principal.account_id, current_id)
            .await?
        {
            store
                .upsert_public_folder_item(
                    UpsertPublicFolderItemInput {
                        id: None,
                        account_id: principal.account_id,
                        public_folder_id: copied.id,
                        item_kind: item.item_kind,
                        message_class: item.message_class,
                        subject: item.subject,
                        body_text: item.body_text,
                        body_html_sanitized: item.body_html_sanitized,
                        source_payload_json: item.source_payload_json,
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-copy-public-folder-item".to_string(),
                        subject: item.id.to_string(),
                    },
                )
                .await?;
        }
        for child in store
            .fetch_public_folder_children(principal.account_id, current_id)
            .await?
        {
            stack.push((child.id, copied.id, None));
        }
    }
    root.ok_or_else(|| anyhow::anyhow!("public folder not found"))
}

pub(super) async fn apply_canonical_public_folder_item_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    item_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let Some(item) = snapshot.public_folder_item_for_id(folder_id, item_id) else {
        return Err(anyhow!("canonical public-folder item was not found"));
    };
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_MESSAGE_CLASS_W,
        MapiValue::String(item.item.message_class.clone()),
    );
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String(item.item.subject.clone()),
    );
    properties.insert(
        PID_TAG_NORMALIZED_SUBJECT_W,
        MapiValue::String(item.item.subject.clone()),
    );
    properties.insert(
        PID_TAG_BODY_W,
        MapiValue::String(item.item.body_text.clone()),
    );
    if let Some(html) = &item.item.body_html_sanitized {
        properties.insert(PID_TAG_BODY_HTML_W, MapiValue::String(html.clone()));
        properties.insert(
            PID_TAG_HTML_BINARY,
            MapiValue::Binary(html.as_bytes().to_vec()),
        );
    }
    apply_mapi_property_values_to_map(&mut properties, values);
    store
        .upsert_public_folder_item(
            UpsertPublicFolderItemInput {
                id: Some(item.item.id),
                account_id: principal.account_id,
                public_folder_id: item.item.public_folder_id,
                item_kind: item.item.item_kind.clone(),
                message_class: optional_pending_text_property(
                    &properties,
                    &[PID_TAG_MESSAGE_CLASS_W],
                )
                .unwrap_or_else(|| "IPM.Post".to_string()),
                subject: pending_text_property(
                    &properties,
                    &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
                ),
                body_text: pending_text_property(&properties, &[PID_TAG_BODY_W]),
                body_html_sanitized: pending_html_property(&properties),
                source_payload_json: item.item.source_payload_json.clone(),
            },
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-update-public-folder-item".to_string(),
                subject: format!("public-folder-item:{}", item.item.id),
            },
        )
        .await?;
    Ok(())
}

pub(super) fn public_folder_handle_properties(
    folder: &lpe_storage::PublicFolder,
    folder_id: u64,
) -> HashMap<u32, MapiValue> {
    let folder = crate::mapi_store::MapiPublicFolder {
        id: folder_id,
        folder: folder.clone(),
        item_count: 0,
        child_count: 0,
    };
    [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ENTRY_ID,
        PID_TAG_RECORD_KEY,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_SUBFOLDERS,
        PID_TAG_ACCESS,
        PID_TAG_RIGHTS,
        PID_TAG_EXTENDED_FOLDER_FLAGS,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
    .into_iter()
    .filter_map(|tag| public_folder_property_value(&folder, tag).map(|value| (tag, value)))
    .collect()
}

pub(super) fn public_folder_per_user_stream(
    states: &[lpe_storage::PublicFolderPerUserState],
) -> Vec<u8> {
    let mut states = states.to_vec();
    states.sort_by(|left, right| left.item_id.cmp(&right.item_id));
    let mut stream = Vec::new();
    stream.extend_from_slice(PUBLIC_FOLDER_PER_USER_STREAM_MAGIC);
    stream.extend_from_slice(&(states.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for state in states.into_iter().take(u16::MAX as usize) {
        stream.extend_from_slice(state.item_id.as_bytes());
        stream.push(state.is_read as u8);
        stream.extend_from_slice(&state.last_seen_change.to_le_bytes());
    }
    stream
}

pub(super) fn public_folder_per_user_patches(
    data: &[u8],
) -> Option<Vec<lpe_storage::PublicFolderPerUserStatePatch>> {
    if data.is_empty() {
        return Some(Vec::new());
    }
    if data.len() < PUBLIC_FOLDER_PER_USER_STREAM_MAGIC.len() + 2
        || &data[..PUBLIC_FOLDER_PER_USER_STREAM_MAGIC.len()]
            != PUBLIC_FOLDER_PER_USER_STREAM_MAGIC.as_slice()
    {
        return None;
    }
    let mut offset = PUBLIC_FOLDER_PER_USER_STREAM_MAGIC.len();
    let count = u16::from_le_bytes(data.get(offset..offset + 2)?.try_into().ok()?) as usize;
    offset += 2;
    let mut patches = Vec::with_capacity(count);
    for _ in 0..count {
        let item_id = uuid::Uuid::from_slice(data.get(offset..offset + 16)?).ok()?;
        offset += 16;
        let is_read = *data.get(offset)? != 0;
        offset += 1;
        let last_seen_change = i64::from_le_bytes(data.get(offset..offset + 8)?.try_into().ok()?);
        offset += 8;
        patches.push(lpe_storage::PublicFolderPerUserStatePatch {
            item_id,
            is_read,
            last_seen_change: Some(last_seen_change),
            private_json: None,
        });
    }
    (offset == data.len()).then_some(patches)
}

pub(super) async fn append_get_per_user_long_term_ids_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    if !matches!(
        input_object(session, handle_slots, request),
        Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
    ) {
        responses.extend_from_slice(&rop_error_response(
            0x60,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let mut long_term_ids = snapshot
        .public_folders()
        .iter()
        .filter_map(|folder| crate::mapi::identity::long_term_id_from_object_id(folder.id))
        .collect::<Vec<_>>();
    if long_term_ids.is_empty() {
        let mut canonical_folder_ids = Vec::new();
        if let Ok(trees) = store.fetch_public_folder_trees(principal.account_id).await {
            let mut pending_folder_ids = trees
                .into_iter()
                .filter_map(|tree| tree.root_folder_id)
                .collect::<Vec<_>>();
            let mut seen_folder_ids = HashSet::new();
            while let Some(folder_id) = pending_folder_ids.pop() {
                if !seen_folder_ids.insert(folder_id) {
                    continue;
                }
                if let Ok(folder) = store
                    .fetch_public_folder(principal.account_id, folder_id)
                    .await
                {
                    canonical_folder_ids.push(folder.id);
                }
                if let Ok(children) = store
                    .fetch_public_folder_children(principal.account_id, folder_id)
                    .await
                {
                    pending_folder_ids.extend(children.into_iter().map(|child| child.id));
                }
            }
        }
        let requests = canonical_folder_ids
            .into_iter()
            .map(|canonical_id| MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::PublicFolder,
                canonical_id,
                reserved_global_counter: None,
                source_key: None,
            })
            .collect::<Vec<_>>();
        if let Ok(records) = store
            .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
            .await
        {
            for record in records {
                crate::mapi::identity::remember_mapi_identity_with_source_key(
                    record.canonical_id,
                    record.object_id,
                    Some(record.source_key),
                );
                if let Some(long_term_id) =
                    crate::mapi::identity::long_term_id_from_object_id(record.object_id)
                {
                    long_term_ids.push(long_term_id);
                }
            }
        }
    }
    responses.extend_from_slice(&rop_get_per_user_long_term_ids_response(
        request,
        &long_term_ids,
    ));
}

pub(super) async fn append_get_per_user_guid_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    if !matches!(
        input_object(session, handle_slots, request),
        Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
    ) {
        responses.extend_from_slice(&rop_error_response(
            0x61,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let Some(folder_id) = request
        .long_term_id()
        .and_then(crate::mapi::identity::object_id_from_long_term_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x61,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let mut public_folder_found = snapshot.public_folder_for_id(folder_id).is_some();
    if !public_folder_found {
        if let Ok(records) = store
            .fetch_mapi_identities_by_object_ids(principal.account_id, &[folder_id])
            .await
        {
            for record in records {
                if record.object_kind == MapiIdentityObjectKind::PublicFolder
                    && store
                        .fetch_public_folder(principal.account_id, record.canonical_id)
                        .await
                        .is_ok()
                {
                    public_folder_found = true;
                    break;
                }
            }
        }
    }
    if !public_folder_found {
        responses.extend_from_slice(&rop_error_response(
            0x61,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    }
    responses.extend_from_slice(&rop_get_per_user_guid_response(
        request,
        &crate::mapi::identity::STORE_REPLICA_GUID,
    ));
}

pub(super) async fn append_read_per_user_information_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let Some(folder_id) = request.per_user_folder_object_id() else {
        responses.extend_from_slice(&rop_error_response(
            0x63,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    };
    let Some(public_folder) = snapshot.public_folder_for_id(folder_id) else {
        responses.extend_from_slice(&rop_error_response(
            0x63,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    };
    let states = match store
        .fetch_public_folder_per_user_state(principal.account_id, public_folder.folder.id)
        .await
    {
        Ok(states) => states,
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x63,
                request.response_handle_index(),
                EC_RULE_NOT_FOUND,
            ));
            return;
        }
    };
    let stream = public_folder_per_user_stream(&states);
    responses.extend_from_slice(&rop_read_per_user_information_response(request, &stream));
}

pub(super) async fn append_write_per_user_information_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let Some(folder_id) = request.per_user_folder_object_id() else {
        responses.extend_from_slice(&rop_error_response(
            0x64,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    };
    let Some(public_folder) = snapshot.public_folder_for_id(folder_id) else {
        responses.extend_from_slice(&rop_error_response(
            0x64,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    };
    if request.per_user_data_offset() != 0 || !request.per_user_has_finished() {
        responses.extend_from_slice(&rop_error_response(
            0x64,
            request.response_handle_index(),
            EC_RULE_INVALID_PARAMETER,
        ));
        return;
    }
    let patches = match public_folder_per_user_patches(request.per_user_write_data()) {
        Some(patches) => patches,
        None => {
            responses.extend_from_slice(&rop_error_response(
                0x64,
                request.response_handle_index(),
                EC_RULE_INVALID_PARAMETER,
            ));
            return;
        }
    };
    if !patches.is_empty()
        && store
            .patch_public_folder_per_user_state(
                principal.account_id,
                public_folder.folder.id,
                &patches,
            )
            .await
            .is_err()
    {
        responses.extend_from_slice(&rop_error_response(
            0x64,
            request.response_handle_index(),
            EC_RULE_INVALID_PARAMETER,
        ));
        return;
    }
    responses.extend_from_slice(&rop_write_per_user_information_response(request));
}

pub(super) fn append_get_owning_servers_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    if !logon_request_handle(session, handle_slots, request) {
        responses.extend_from_slice(&rop_error_response(
            0x42,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let Some(folder_id) = request.public_folder_probe_object_id() else {
        responses.extend_from_slice(&rop_error_response(
            0x42,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    };
    if folder_id != PUBLIC_FOLDERS_ROOT_FOLDER_ID
        && snapshot.public_folder_for_id(folder_id).is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x42,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    }
    let servers = snapshot.public_folder_replica_server_names(folder_id);
    responses.extend_from_slice(&rop_get_owning_servers_response(request, &servers))
}

pub(super) fn append_public_folder_is_ghosted_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    if !logon_request_handle(session, handle_slots, request) {
        responses.extend_from_slice(&rop_error_response(
            0x45,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let Some(folder_id) = request.public_folder_probe_object_id() else {
        responses.extend_from_slice(&rop_error_response(
            0x45,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    };
    if folder_id != PUBLIC_FOLDERS_ROOT_FOLDER_ID
        && snapshot.public_folder_for_id(folder_id).is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x45,
            request.response_handle_index(),
            EC_RULE_NOT_FOUND,
        ));
        return;
    }
    let is_ghosted = folder_id != PUBLIC_FOLDERS_ROOT_FOLDER_ID
        && snapshot
            .public_folder_replica_server_names(folder_id)
            .is_empty();
    responses.extend_from_slice(&rop_public_folder_is_ghosted_response(request, is_ghosted))
}
