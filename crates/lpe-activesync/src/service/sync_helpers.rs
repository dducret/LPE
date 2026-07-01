use anyhow::Result;
use serde_json::Value;

use crate::{
    constants::{CALENDAR_CLASS, CONTACTS_CLASS, ROOT_FOLDER_ID},
    snapshot::{drafts_collection, mail_collection},
    types::{CollectionDefinition, CollectionStateEntry, SnapshotChange, StoredSyncState},
    wbxml::WbxmlNode,
};
pub(super) fn decode_sync_state(snapshot_json: &str) -> Result<StoredSyncState> {
    if let Ok(state) = serde_json::from_str::<StoredSyncState>(snapshot_json) {
        return Ok(state);
    }

    let legacy_snapshot: Value = serde_json::from_str(snapshot_json)?;
    let collection_state = match legacy_snapshot {
        Value::Array(entries) => entries
            .into_iter()
            .filter_map(|entry| {
                let object = entry.as_object()?;
                Some(CollectionStateEntry {
                    server_id: object.get("id")?.as_str()?.to_string(),
                    fingerprint: object.get("fingerprint")?.as_str()?.to_string(),
                })
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    Ok(completed_sync_state(collection_state, None))
}

pub(super) fn completed_sync_state(
    collection_state: Vec<CollectionStateEntry>,
    hierarchy_generation: Option<String>,
) -> StoredSyncState {
    StoredSyncState {
        hierarchy_generation,
        collection_state,
        pending_changes: Vec::new(),
        next_offset: 0,
    }
}

pub(super) fn has_client_commands(collection_node: &WbxmlNode) -> bool {
    collection_node
        .child("Commands")
        .map(|commands| !commands.children.is_empty())
        .unwrap_or(false)
}

pub(super) fn sync_collection_status_node(collection_id: Option<&str>, status: &str) -> WbxmlNode {
    let mut collection = WbxmlNode::new(0, "Collection");
    if let Some(collection_id) = collection_id {
        collection.push(WbxmlNode::with_text(0, "CollectionId", collection_id));
    }
    collection.push(WbxmlNode::with_text(0, "Status", status));
    collection
}

pub(super) fn sync_collection_has_unsupported_command(
    collection_node: &WbxmlNode,
    collection: &CollectionDefinition,
) -> bool {
    let Some(commands) = collection_node.child("Commands") else {
        return false;
    };
    commands
        .children
        .iter()
        .any(|command| !sync_command_supported_for_collection(command.name.as_str(), collection))
}

fn sync_command_supported_for_collection(command: &str, collection: &CollectionDefinition) -> bool {
    if drafts_collection(collection) {
        return matches!(command, "Add" | "Change" | "Delete");
    }
    if mail_collection(collection) {
        return matches!(command, "Change" | "Delete");
    }
    if collection.class_name == CONTACTS_CLASS || collection.class_name == CALENDAR_CLASS {
        return matches!(command, "Add" | "Change" | "Delete");
    }
    false
}

pub(super) fn pending_page(
    changes: &[SnapshotChange],
    offset: usize,
    window_size: u64,
) -> (Vec<SnapshotChange>, usize) {
    let end = (offset + window_size as usize).min(changes.len());
    (changes[offset..end].to_vec(), end)
}

pub(super) fn value_to_wbxml(value: Value) -> WbxmlNode {
    match value {
        Value::Object(map) => crate::snapshot::value_to_node(&map),
        _ => WbxmlNode::new(0, "ApplicationData"),
    }
}

pub(super) fn hierarchy_generation(collections: &[CollectionDefinition]) -> String {
    let mut entries = collections
        .iter()
        .map(|collection| {
            format!(
                "{}|{}:{}:{}:{}",
                collection.id,
                collection.class_name,
                collection.parent_id.as_deref().unwrap_or(ROOT_FOLDER_ID),
                collection.display_name,
                collection.folder_type.as_str()
            )
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries.join("\n")
}

pub(super) fn hierarchy_generation_from_snapshot(snapshot: &Value) -> String {
    let mut entries = match snapshot {
        Value::Array(entries) => entries
            .iter()
            .filter_map(|entry| {
                let object = entry.as_object()?;
                Some(format!(
                    "{}|{}",
                    object.get("id")?.as_str()?,
                    object.get("fingerprint")?.as_str()?
                ))
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    entries.sort();
    entries.join("\n")
}
