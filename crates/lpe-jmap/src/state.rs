use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QueryStateToken {
    pub(crate) version: String,
    pub(crate) kind: String,
    pub(crate) filter: Option<Value>,
    pub(crate) sort: Option<Vec<Value>>,
    pub(crate) ids: Vec<String>,
}

#[derive(Debug, Default)]
pub(crate) struct QueryDiff {
    pub(crate) removed: Vec<String>,
    pub(crate) added: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StateToken {
    pub(crate) version: String,
    pub(crate) kind: String,
    pub(crate) entries: Vec<StateEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct StateEntry {
    pub(crate) id: String,
    pub(crate) fingerprint: String,
}

pub(crate) fn changes_response(
    account_id: Uuid,
    kind: &str,
    since_state: &str,
    max_changes: Option<u64>,
    current_entries: Vec<StateEntry>,
) -> Value {
    let max_changes = max_changes.unwrap_or(u64::MAX) as usize;
    let new_state =
        encode_state(kind, current_entries.clone()).unwrap_or_else(|_| crate::SESSION_STATE.to_string());
    let Ok(previous) = decode_state(since_state) else {
        let created = current_entries
            .into_iter()
            .take(max_changes)
            .map(|entry| entry.id)
            .collect::<Vec<_>>();
        return json!({
            "accountId": account_id.to_string(),
            "oldState": since_state,
            "newState": new_state,
            "hasMoreChanges": false,
            "created": created,
            "updated": Vec::<String>::new(),
            "destroyed": Vec::<String>::new(),
        });
    };

    if previous.kind != kind {
        let created = current_entries
            .into_iter()
            .take(max_changes)
            .map(|entry| entry.id)
            .collect::<Vec<_>>();
        return json!({
            "accountId": account_id.to_string(),
            "oldState": since_state,
            "newState": new_state,
            "hasMoreChanges": false,
            "created": created,
            "updated": Vec::<String>::new(),
            "destroyed": Vec::<String>::new(),
        });
    }

    let previous_map = previous
        .entries
        .into_iter()
        .map(|entry| (entry.id, entry.fingerprint))
        .collect::<HashMap<_, _>>();
    let current_map = current_entries
        .into_iter()
        .map(|entry| (entry.id, entry.fingerprint))
        .collect::<HashMap<_, _>>();

    let mut created = current_map
        .keys()
        .filter(|id| !previous_map.contains_key(*id))
        .cloned()
        .collect::<Vec<_>>();
    let mut updated = current_map
        .iter()
        .filter_map(|(id, fingerprint)| {
            previous_map
                .get(id)
                .filter(|previous| *previous != fingerprint)
                .map(|_| id.clone())
        })
        .collect::<Vec<_>>();
    let mut destroyed = previous_map
        .keys()
        .filter(|id| !current_map.contains_key(*id))
        .cloned()
        .collect::<Vec<_>>();

    created.sort();
    updated.sort();
    destroyed.sort();

    let total_changes = created.len() + updated.len() + destroyed.len();
    let has_more_changes = total_changes > max_changes;
    if total_changes > max_changes {
        let mut remaining = max_changes;
        created.truncate(remaining.min(created.len()));
        remaining = remaining.saturating_sub(created.len());
        updated.truncate(remaining.min(updated.len()));
        remaining = remaining.saturating_sub(updated.len());
        destroyed.truncate(remaining.min(destroyed.len()));
    }

    json!({
        "accountId": account_id.to_string(),
        "oldState": since_state,
        "newState": new_state,
        "hasMoreChanges": has_more_changes,
        "created": created,
        "updated": updated,
        "destroyed": destroyed,
    })
}

pub(crate) fn encode_state(kind: &str, entries: Vec<StateEntry>) -> Result<String> {
    let mut entries = entries;
    entries.sort_by(|left, right| left.id.cmp(&right.id));
    let token = StateToken {
        version: crate::STATE_TOKEN_VERSION.to_string(),
        kind: kind.to_string(),
        entries,
    };
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token)?))
}

pub(crate) fn decode_state(value: &str) -> Result<StateToken> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| anyhow!("invalid state"))?;
    let token: StateToken = serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid state"))?;
    if token.version != crate::STATE_TOKEN_VERSION {
        bail!("unsupported state version");
    }
    Ok(token)
}

pub(crate) fn encode_push_state(
    type_states: &HashMap<String, HashMap<String, String>>,
) -> Result<String> {
    let mut entries = type_states
        .iter()
        .flat_map(|(account_id, states)| {
            states.iter().map(move |(data_type, state)| StateEntry {
                id: format!("{account_id}:{data_type}"),
                fingerprint: state.clone(),
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.id.cmp(&right.id));
    encode_state("Push", entries)
}

pub(crate) fn encode_query_state(
    kind: &str,
    filter: Option<Value>,
    sort: Option<Vec<Value>>,
    ids: Vec<String>,
) -> Result<String> {
    let token = QueryStateToken {
        version: crate::QUERY_STATE_VERSION.to_string(),
        kind: kind.to_string(),
        filter,
        sort,
        ids,
    };
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token)?))
}

pub(crate) fn decode_query_state(value: &str) -> Result<QueryStateToken> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| anyhow!("invalid queryState"))?;
    let token: QueryStateToken =
        serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid queryState"))?;
    if token.version != crate::QUERY_STATE_VERSION {
        bail!("unsupported queryState version");
    }
    Ok(token)
}

pub(crate) fn query_changes_response(
    account_id: Uuid,
    kind: &str,
    since_query_state: String,
    filter: Option<Value>,
    sort: Option<Vec<Value>>,
    current_ids: Vec<String>,
    total: u64,
    max_changes: Option<u64>,
) -> Result<Value> {
    let previous = decode_query_state(&since_query_state)?;
    if previous.kind != kind {
        bail!("queryState does not match requested method");
    }
    if previous.filter != filter || previous.sort != sort {
        bail!("queryState does not match requested filter or sort");
    }

    let next_query_state = encode_query_state(kind, filter, sort, current_ids.clone())?;
    let diff = if kind == "Task" {
        compute_query_diff_with_reorders(&previous.ids, &current_ids, max_changes)
    } else {
        compute_query_diff(&previous.ids, &current_ids, max_changes)
    };
    let change_count = diff.removed.len() + diff.added.len();
    let change_limit = max_changes.unwrap_or(u64::MAX) as usize;

    Ok(json!({
        "accountId": account_id.to_string(),
        "oldQueryState": since_query_state,
        "newQueryState": next_query_state,
        "removed": diff.removed,
        "added": diff.added,
        "total": total,
        "hasMoreChanges": change_count >= change_limit && change_limit != usize::MAX,
    }))
}

pub(crate) fn compute_query_diff(
    previous_ids: &[String],
    current_ids: &[String],
    max_changes: Option<u64>,
) -> QueryDiff {
    let mut removed = Vec::new();
    let mut added = Vec::new();
    let max_changes = max_changes.unwrap_or(u64::MAX) as usize;

    for id in previous_ids {
        if !current_ids.contains(id) {
            removed.push(id.clone());
            if removed.len() + added.len() >= max_changes {
                return QueryDiff { removed, added };
            }
        }
    }

    for (index, id) in current_ids.iter().enumerate() {
        if !previous_ids.contains(id) {
            added.push(json!({
                "id": id,
                "index": index,
            }));
            if removed.len() + added.len() >= max_changes {
                break;
            }
        }
    }

    QueryDiff { removed, added }
}

pub(crate) fn compute_query_diff_with_reorders(
    previous_ids: &[String],
    current_ids: &[String],
    max_changes: Option<u64>,
) -> QueryDiff {
    let mut removed = Vec::new();
    let mut added = Vec::new();
    let max_changes = max_changes.unwrap_or(u64::MAX) as usize;
    let previous_positions = previous_ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect::<HashMap<_, _>>();
    let current_positions = current_ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect::<HashMap<_, _>>();

    for (index, id) in previous_ids.iter().enumerate() {
        let moved = current_positions
            .get(id.as_str())
            .is_some_and(|current_index| *current_index != index);
        if !current_positions.contains_key(id.as_str()) || moved {
            removed.push(id.clone());
            if removed.len() + added.len() >= max_changes {
                return QueryDiff { removed, added };
            }
        }
    }

    for (index, id) in current_ids.iter().enumerate() {
        let moved = previous_positions
            .get(id.as_str())
            .is_some_and(|previous_index| *previous_index != index);
        if !previous_positions.contains_key(id.as_str()) || moved {
            added.push(json!({
                "id": id,
                "index": index,
            }));
            if removed.len() + added.len() >= max_changes {
                break;
            }
        }
    }

    QueryDiff { removed, added }
}
