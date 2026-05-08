use anyhow::Result;
use axum::response::{
    sse::{Event, KeepAlive, Sse},
    IntoResponse, Response,
};
use lpe_storage::{AuthenticatedAccount, Storage};
use serde::Deserialize;
use std::{collections::HashMap, convert::Infallible, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    protocol::WebSocketStateChange,
    state::encode_push_state,
    store::JmapStore,
    websocket::{finalize_push_change, normalize_push_data_types, PushSubscription},
    JmapService,
};

pub(crate) type EventSourceStream = Response;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EventSourceQuery {
    pub types: Option<String>,
    pub closeafter: Option<String>,
    pub close_after: Option<String>,
    pub push_state: Option<String>,
}

impl<V> JmapService<Storage, V>
where
    V: lpe_magika::Detector,
{
    pub(crate) async fn handle_event_source(
        &self,
        account: AuthenticatedAccount,
        query: EventSourceQuery,
        last_event_id: Option<String>,
    ) -> Result<EventSourceStream> {
        let enabled_types =
            normalize_push_data_types(event_source_data_types(query.types.as_deref()));
        let mut listener = self.store.create_push_listener(account.account_id).await?;
        let current_cursor = self
            .store
            .fetch_canonical_change_cursor(account.account_id)
            .await?;
        let current_type_states = self
            .current_push_states(account.account_id, &enabled_types)
            .await?;
        let current_push_state = encode_push_state(&current_type_states, current_cursor)?;
        let client_push_state = query.push_state.or(last_event_id);
        let recovered_change = self
            .recover_push_enable_change(
                account.account_id,
                &enabled_types,
                client_push_state.as_deref(),
                current_cursor,
                &current_type_states,
            )
            .await?;

        let mut subscription = PushSubscription {
            enabled_types,
            last_type_states: current_type_states,
            last_push_state: Some(current_push_state.clone()),
            last_journal_cursor: current_cursor,
        };
        let close_after =
            event_source_close_after(query.close_after.as_deref().or(query.closeafter.as_deref()));
        let (sender, receiver) = mpsc::channel(16);
        let service = JmapService {
            store: self.store.clone(),
            validator: self.validator.clone(),
        };

        tokio::spawn(async move {
            let mut sent_events = 0usize;
            let initial_changed = recovered_change.unwrap_or_default();
            if send_state_change_event(&sender, initial_changed, current_push_state.clone())
                .await
                .is_err()
            {
                return;
            }
            sent_events += 1;
            if close_after.is_some_and(|limit| sent_events >= limit) {
                return;
            }

            loop {
                let categories = service.push_categories(&subscription.enabled_types);
                let Ok(change_set) = listener.wait_for_change(&categories).await else {
                    break;
                };
                let Ok((changed, current_type_states)) = service
                    .compute_push_changes(account.account_id, &mut subscription, &change_set)
                    .await
                else {
                    continue;
                };
                let Ok(Some((changed, push_state))) = finalize_push_change(
                    &mut subscription,
                    changed,
                    current_type_states,
                    change_set.journal_cursor(),
                ) else {
                    continue;
                };
                if send_state_change_event(&sender, changed, push_state)
                    .await
                    .is_err()
                {
                    break;
                }
                sent_events += 1;
                if close_after.is_some_and(|limit| sent_events >= limit) {
                    break;
                }
            }
        });

        Ok(Sse::new(ReceiverStream::new(receiver))
            .keep_alive(
                KeepAlive::new()
                    .interval(Duration::from_secs(30))
                    .text("keepalive"),
            )
            .into_response())
    }
}

fn event_source_data_types(types: Option<&str>) -> Option<Vec<String>> {
    types
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
}

fn event_source_close_after(value: Option<&str>) -> Option<usize> {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

async fn send_state_change_event(
    sender: &mpsc::Sender<std::result::Result<Event, Infallible>>,
    changed: HashMap<String, HashMap<String, String>>,
    push_state: String,
) -> Result<()> {
    let payload = WebSocketStateChange {
        type_name: "StateChange",
        changed,
        push_state: Some(push_state.clone()),
    };
    let event = Event::default()
        .event("StateChange")
        .id(push_state)
        .data(serde_json::to_string(&payload)?);
    sender.send(Ok(event)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn event_source_types_use_all_supported_types_when_missing() {
        assert_eq!(event_source_data_types(None), None);
    }

    #[test]
    fn event_source_types_parse_comma_separated_values() {
        assert_eq!(
            event_source_data_types(Some("Mailbox, Email,Unsupported")),
            Some(vec![
                "Mailbox".to_string(),
                "Email".to_string(),
                "Unsupported".to_string()
            ])
        );
        assert_eq!(
            normalize_push_data_types(event_source_data_types(Some("Mailbox, Email,Unsupported"))),
            HashSet::from(["Mailbox".to_string(), "Email".to_string()])
        );
    }
}
