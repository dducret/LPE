use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QueryStateToken {
    pub(crate) version: String,
    pub(crate) account_id: String,
    pub(crate) kind: String,
    pub(crate) filter: Option<Value>,
    pub(crate) sort: Option<Vec<Value>>,
    pub(crate) ids: Vec<String>,
}

#[derive(Debug, Default)]
pub(crate) struct QueryDiff {
    pub(crate) removed: Vec<String>,
    pub(crate) added: Vec<Value>,
    pub(crate) has_more_changes: bool,
    pub(crate) query_state_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StateToken {
    pub(crate) version: String,
    pub(crate) account_id: String,
    pub(crate) kind: String,
    pub(crate) entries: Vec<StateEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PushStateToken {
    pub(crate) version: String,
    pub(crate) cursor: Option<i64>,
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
) -> Result<Value> {
    let max_changes = max_changes.unwrap_or(u64::MAX).max(1) as usize;
    let previous_entries = if since_state == "0" {
        Vec::new()
    } else {
        let previous = decode_state(since_state)?;
        if previous.account_id != account_id.to_string() {
            bail!("state does not match requested account");
        }
        if previous.kind != kind {
            bail!("state does not match requested method");
        }
        previous.entries
    };

    let previous_map = previous_entries
        .iter()
        .cloned()
        .into_iter()
        .map(|entry| (entry.id, entry.fingerprint))
        .collect::<HashMap<_, _>>();
    let current_map = current_entries
        .iter()
        .cloned()
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
    if has_more_changes {
        let mut remaining = max_changes;
        created.truncate(remaining.min(created.len()));
        remaining = remaining.saturating_sub(created.len());
        updated.truncate(remaining.min(updated.len()));
        remaining = remaining.saturating_sub(updated.len());
        destroyed.truncate(remaining.min(destroyed.len()));
    }

    let new_state_entries = if has_more_changes {
        apply_state_changes(
            previous_entries,
            &current_map,
            &created,
            &updated,
            &destroyed,
        )
    } else {
        current_entries
    };
    let new_state = encode_state(account_id, kind, new_state_entries)
        .unwrap_or_else(|_| crate::SESSION_STATE.to_string());

    Ok(json!({
        "accountId": account_id.to_string(),
        "oldState": since_state,
        "newState": new_state,
        "hasMoreChanges": has_more_changes,
        "created": created,
        "updated": updated,
        "destroyed": destroyed,
    }))
}

fn apply_state_changes(
    previous_entries: Vec<StateEntry>,
    current_map: &HashMap<String, String>,
    created: &[String],
    updated: &[String],
    destroyed: &[String],
) -> Vec<StateEntry> {
    let mut entries = previous_entries
        .into_iter()
        .filter(|entry| !destroyed.contains(&entry.id))
        .map(|mut entry| {
            if updated.contains(&entry.id) {
                if let Some(fingerprint) = current_map.get(&entry.id) {
                    entry.fingerprint = fingerprint.clone();
                }
            }
            (entry.id.clone(), entry)
        })
        .collect::<HashMap<_, _>>();

    for id in created {
        if let Some(fingerprint) = current_map.get(id) {
            entries.insert(
                id.clone(),
                StateEntry {
                    id: id.clone(),
                    fingerprint: fingerprint.clone(),
                },
            );
        }
    }

    entries.into_values().collect()
}

pub(crate) fn encode_state(
    account_id: Uuid,
    kind: &str,
    entries: Vec<StateEntry>,
) -> Result<String> {
    let mut entries = entries;
    entries.sort_by(|left, right| left.id.cmp(&right.id));
    let token = StateToken {
        version: crate::STATE_TOKEN_VERSION.to_string(),
        account_id: account_id.to_string(),
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
    cursor: Option<i64>,
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
    let token = PushStateToken {
        version: crate::PUSH_STATE_VERSION.to_string(),
        cursor,
        entries,
    };
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token)?))
}

pub(crate) fn decode_push_state(value: &str) -> Result<PushStateToken> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| anyhow!("invalid pushState"))?;
    if let Ok(token) = serde_json::from_slice::<PushStateToken>(&bytes) {
        if token.version != crate::PUSH_STATE_VERSION {
            bail!("unsupported pushState version");
        }
        return Ok(token);
    }

    let token: StateToken =
        serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid pushState"))?;
    if token.version != crate::STATE_TOKEN_VERSION || token.kind != "Push" {
        bail!("invalid pushState");
    }

    Ok(PushStateToken {
        version: crate::PUSH_STATE_VERSION.to_string(),
        cursor: None,
        entries: token.entries,
    })
}

pub(crate) fn push_state_entries_to_types(
    entries: &[StateEntry],
) -> HashMap<String, HashMap<String, String>> {
    let mut type_states = HashMap::new();
    for entry in entries {
        let Some((account_id, data_type)) = entry.id.split_once(':') else {
            continue;
        };
        type_states
            .entry(account_id.to_string())
            .or_insert_with(HashMap::new)
            .insert(data_type.to_string(), entry.fingerprint.clone());
    }
    type_states
}

pub(crate) fn encode_query_state(
    account_id: Uuid,
    kind: &str,
    filter: Option<Value>,
    sort: Option<Vec<Value>>,
    ids: Vec<String>,
) -> Result<String> {
    let token = QueryStateToken {
        version: crate::QUERY_STATE_VERSION.to_string(),
        account_id: account_id.to_string(),
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
    if previous.account_id != account_id.to_string() {
        bail!("queryState does not match requested account");
    }
    if previous.kind != kind {
        bail!("queryState does not match requested method");
    }
    if previous.filter != filter || previous.sort != sort {
        bail!("queryState does not match requested filter or sort");
    }

    let diff = if matches!(
        kind,
        "Email/query"
            | "Thread/query"
            | "Task"
            | "Mailbox/query"
            | "AddressBook/query"
            | "Calendar/query"
            | "ContactCard"
            | "CalendarEvent"
    ) {
        compute_query_diff_with_reorders(&previous.ids, &current_ids, max_changes)
    } else {
        compute_query_diff(&previous.ids, &current_ids, max_changes)
    };
    let next_query_state =
        encode_query_state(account_id, kind, filter, sort, diff.query_state_ids.clone())?;

    Ok(json!({
        "accountId": account_id.to_string(),
        "oldQueryState": since_query_state,
        "newQueryState": next_query_state,
        "removed": diff.removed,
        "added": diff.added,
        "total": total,
        "hasMoreChanges": diff.has_more_changes,
    }))
}

pub(crate) fn compute_query_diff(
    previous_ids: &[String],
    current_ids: &[String],
    max_changes: Option<u64>,
) -> QueryDiff {
    let mut removed = previous_ids
        .iter()
        .filter(|id| !current_ids.contains(id))
        .cloned()
        .collect::<Vec<_>>();
    let mut added = current_ids
        .iter()
        .enumerate()
        .filter(|(_, id)| !previous_ids.contains(id))
        .map(|(index, id)| {
            json!({
                "id": id,
                "index": index,
            })
        })
        .collect::<Vec<_>>();
    truncate_query_diff(
        previous_ids,
        current_ids,
        &mut removed,
        &mut added,
        max_changes,
    )
}

fn truncate_query_diff(
    previous_ids: &[String],
    current_ids: &[String],
    removed: &mut Vec<String>,
    added: &mut Vec<Value>,
    max_changes: Option<u64>,
) -> QueryDiff {
    let change_limit = max_changes.unwrap_or(u64::MAX).max(1) as usize;
    let total_changes = removed.len() + added.len();
    let has_more_changes = total_changes > change_limit;
    if total_changes > change_limit {
        let mut remaining = change_limit;
        removed.truncate(remaining.min(removed.len()));
        remaining = remaining.saturating_sub(removed.len());
        added.truncate(remaining.min(added.len()));
    }
    let query_state_ids = if has_more_changes {
        apply_query_changes(previous_ids, removed, added)
    } else {
        current_ids.to_vec()
    };
    QueryDiff {
        removed: std::mem::take(removed),
        added: std::mem::take(added),
        has_more_changes,
        query_state_ids,
    }
}

fn apply_query_changes(
    previous_ids: &[String],
    removed: &[String],
    added: &[Value],
) -> Vec<String> {
    let mut ids = previous_ids
        .iter()
        .filter(|id| !removed.contains(id))
        .cloned()
        .collect::<Vec<_>>();

    for value in added {
        let Some(id) = value.get("id").and_then(Value::as_str) else {
            continue;
        };
        let index = value
            .get("index")
            .and_then(Value::as_u64)
            .unwrap_or(ids.len() as u64) as usize;
        ids.retain(|existing| existing != id);
        ids.insert(index.min(ids.len()), id.to_string());
    }

    ids
}

pub(crate) fn compute_query_diff_with_reorders(
    previous_ids: &[String],
    current_ids: &[String],
    max_changes: Option<u64>,
) -> QueryDiff {
    let mut removed = Vec::new();
    let mut added = Vec::new();
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
        }
    }

    truncate_query_diff(
        previous_ids,
        current_ids,
        &mut removed,
        &mut added,
        max_changes,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, fingerprint: &str) -> StateEntry {
        StateEntry {
            id: id.to_string(),
            fingerprint: fingerprint.to_string(),
        }
    }

    #[test]
    fn changes_response_returns_intermediate_state_when_truncated() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let old_state = encode_state(
            account_id,
            "Email",
            vec![entry("a", "old"), entry("b", "same"), entry("c", "gone")],
        )
        .unwrap();

        let first = changes_response(
            account_id,
            "Email",
            &old_state,
            Some(1),
            vec![entry("a", "new"), entry("b", "same"), entry("d", "created")],
        );
        let first = first.unwrap();
        assert_eq!(first["hasMoreChanges"], Value::Bool(true));
        assert_eq!(first["created"].as_array().unwrap().len(), 1);

        let second = changes_response(
            account_id,
            "Email",
            first["newState"].as_str().unwrap(),
            Some(1),
            vec![entry("a", "new"), entry("b", "same"), entry("d", "created")],
        );
        let second = second.unwrap();
        assert_eq!(second["hasMoreChanges"], Value::Bool(true));
        assert_eq!(second["updated"].as_array().unwrap().len(), 1);

        let third = changes_response(
            account_id,
            "Email",
            second["newState"].as_str().unwrap(),
            Some(1),
            vec![entry("a", "new"), entry("b", "same"), entry("d", "created")],
        );
        let third = third.unwrap();
        assert_eq!(third["hasMoreChanges"], Value::Bool(false));
        assert_eq!(third["destroyed"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn changes_response_rejects_invalid_or_mismatched_state_tokens() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let other_account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
        let other_account_state = encode_state(other_account_id, "Email", Vec::new()).unwrap();
        let wrong_kind_state = encode_state(account_id, "Mailbox", Vec::new()).unwrap();

        let initial =
            changes_response(account_id, "Email", "0", None, vec![entry("a", "1")]).unwrap();
        assert_eq!(initial["created"], json!(["a"]));

        assert!(changes_response(account_id, "Email", "not-a-state", None, Vec::new()).is_err());
        assert!(
            changes_response(account_id, "Email", &other_account_state, None, Vec::new()).is_err()
        );
        assert!(
            changes_response(account_id, "Email", &wrong_kind_state, None, Vec::new()).is_err()
        );
    }

    #[test]
    fn query_changes_response_returns_intermediate_query_state_when_truncated() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let old_query_state = encode_query_state(
            account_id,
            "Email/query",
            None,
            None,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();
        let current_ids = vec!["b".to_string(), "d".to_string(), "e".to_string()];

        let first = query_changes_response(
            account_id,
            "Email/query",
            old_query_state,
            None,
            None,
            current_ids.clone(),
            current_ids.len() as u64,
            Some(1),
        )
        .unwrap();
        assert_eq!(first["hasMoreChanges"], Value::Bool(true));
        assert_eq!(first["removed"].as_array().unwrap().len(), 1);

        let second = query_changes_response(
            account_id,
            "Email/query",
            first["newQueryState"].as_str().unwrap().to_string(),
            None,
            None,
            current_ids.clone(),
            current_ids.len() as u64,
            Some(1),
        )
        .unwrap();
        assert_eq!(second["hasMoreChanges"], Value::Bool(true));
        assert_eq!(second["removed"].as_array().unwrap().len(), 1);

        let third = query_changes_response(
            account_id,
            "Email/query",
            second["newQueryState"].as_str().unwrap().to_string(),
            None,
            None,
            current_ids.clone(),
            current_ids.len() as u64,
            Some(2),
        )
        .unwrap();
        assert_eq!(third["hasMoreChanges"], Value::Bool(false));
        assert_eq!(third["added"].as_array().unwrap().len(), 2);
        assert_eq!(
            decode_query_state(third["newQueryState"].as_str().unwrap())
                .unwrap()
                .ids,
            current_ids
        );
    }

    #[test]
    fn email_query_changes_reports_reorders_and_paginates_to_current_order() {
        let account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let old_query_state = encode_query_state(
            account_id,
            "Email/query",
            None,
            None,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .unwrap();
        let current_ids = vec!["c".to_string(), "a".to_string(), "b".to_string()];

        let first = query_changes_response(
            account_id,
            "Email/query",
            old_query_state,
            None,
            None,
            current_ids.clone(),
            current_ids.len() as u64,
            Some(2),
        )
        .unwrap();
        assert_eq!(first["hasMoreChanges"], Value::Bool(true));
        assert_eq!(first["removed"], json!(["a".to_string(), "b".to_string()]));
        assert_eq!(first["added"], json!([]));

        let second = query_changes_response(
            account_id,
            "Email/query",
            first["newQueryState"].as_str().unwrap().to_string(),
            None,
            None,
            current_ids.clone(),
            current_ids.len() as u64,
            Some(4),
        )
        .unwrap();
        assert_eq!(second["hasMoreChanges"], Value::Bool(false));
        assert_eq!(second["removed"], json!([]));
        assert_eq!(
            second["added"],
            json!([
                {"id": "a", "index": 1},
                {"id": "b", "index": 2}
            ])
        );
        assert_eq!(
            decode_query_state(second["newQueryState"].as_str().unwrap())
                .unwrap()
                .ids,
            current_ids
        );
    }
}
