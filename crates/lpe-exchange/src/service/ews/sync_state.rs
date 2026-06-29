use super::super::*;

const COLLABORATION_SYNC_STATE_VERSION: &str = "v2";

pub(in crate::service) fn collaboration_sync_state(
    kind: &str,
    collection_id: &str,
    items: &[(Uuid, String)],
) -> String {
    let item_list = items
        .iter()
        .map(|(id, change_key)| format!("{id}={change_key}"))
        .collect::<Vec<_>>()
        .join(",");
    if item_list.is_empty() {
        format!("{kind}:{collection_id}:{COLLABORATION_SYNC_STATE_VERSION}:0")
    } else {
        format!("{kind}:{collection_id}:{COLLABORATION_SYNC_STATE_VERSION}:{item_list}")
    }
}

#[derive(Debug, Clone)]
pub(in crate::service) struct SyncStateItem {
    pub(in crate::service) id: Uuid,
    pub(in crate::service) change_key: Option<String>,
}

#[derive(Debug, Clone)]
pub(in crate::service) struct CollaborationSyncState {
    pub(in crate::service) is_current_version: bool,
    pub(in crate::service) items: Vec<SyncStateItem>,
}

impl Default for CollaborationSyncState {
    fn default() -> Self {
        Self {
            is_current_version: true,
            items: Vec::new(),
        }
    }
}

pub(in crate::service) fn collaboration_sync_state_items(
    sync_state: &str,
    kind: &str,
    collection_id: &str,
) -> CollaborationSyncState {
    let prefix = format!("{kind}:{collection_id}:");
    let Some(values) = sync_state.strip_prefix(&prefix) else {
        return CollaborationSyncState::default();
    };
    let (is_current_version, values) = if let Some(values) =
        values.strip_prefix(&format!("{COLLABORATION_SYNC_STATE_VERSION}:"))
    {
        (true, values)
    } else {
        (false, values)
    };
    let items = values
        .split(',')
        .filter(|value| !value.is_empty() && *value != "0")
        .filter_map(|value| {
            if let Some((id, change_key)) = value.split_once('=') {
                return Uuid::parse_str(id).ok().map(|id| SyncStateItem {
                    id,
                    change_key: Some(change_key.to_string()),
                });
            }
            Uuid::parse_str(value).ok().map(|id| SyncStateItem {
                id,
                change_key: None,
            })
        })
        .collect();
    CollaborationSyncState {
        is_current_version,
        items,
    }
}

pub(in crate::service) fn collaboration_sync_state_collection_id<'a>(
    sync_state: &'a str,
    kind: &str,
) -> Option<&'a str> {
    sync_state
        .strip_prefix(&format!("{kind}:"))?
        .split(':')
        .next()
}

pub(in crate::service) fn sync_state_items_by_id(
    items: &[SyncStateItem],
) -> HashMap<Uuid, Option<String>> {
    items
        .iter()
        .map(|item| (item.id, item.change_key.clone()))
        .collect()
}

pub(in crate::service) fn sync_version_by_id(items: Vec<(Uuid, String)>) -> HashMap<Uuid, String> {
    items.into_iter().collect()
}
