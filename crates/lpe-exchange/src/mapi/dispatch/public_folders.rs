const PUBLIC_FOLDER_PER_USER_STREAM_MAGIC: &[u8; 8] = b"LPEPFU1\0";

use super::*;

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
