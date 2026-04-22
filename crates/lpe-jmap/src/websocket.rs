use anyhow::Result;
use axum::{
    extract::ws::{Message, WebSocket},
    http::StatusCode,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use lpe_storage::{AuthenticatedAccount, CanonicalChangeCategory, CanonicalPushChangeSet};

use crate::{
    protocol::{
        JmapApiRequest, WebSocketPushDisable, WebSocketPushEnable, WebSocketRequestEnvelope,
        WebSocketRequestError, WebSocketResponse, WebSocketStateChange,
    },
    state::encode_push_state,
    store::JmapPushListener,
    JmapService,
};

#[derive(Debug, Default)]
pub(crate) struct PushSubscription {
    pub(crate) enabled_types: HashSet<String>,
    pub(crate) last_type_states: HashMap<String, HashMap<String, String>>,
    pub(crate) last_push_state: Option<String>,
}

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_websocket(
        &self,
        mut socket: WebSocket,
        account: AuthenticatedAccount,
    ) {
        let mut subscription = PushSubscription::default();
        let Ok(mut listener) = self.store.create_push_listener(account.account_id).await else {
            return;
        };

        loop {
            let push_categories = self.push_categories(&subscription.enabled_types);
            tokio::select! {
                incoming = socket.recv() => {
                    let Some(Ok(message)) = incoming else {
                        break;
                    };
                    if self
                        .handle_websocket_message(&mut socket, &account, &mut subscription, message)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                changed = listener.wait_for_change(&push_categories), if !subscription.enabled_types.is_empty() => {
                    let Ok(change_set) = changed else {
                        break;
                    };
                    if self
                        .publish_state_changes(
                            &mut socket,
                            account.account_id,
                            &mut subscription,
                            &change_set,
                        )
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    }

    async fn handle_websocket_message(
        &self,
        socket: &mut WebSocket,
        account: &AuthenticatedAccount,
        subscription: &mut PushSubscription,
        message: Message,
    ) -> Result<()> {
        match message {
            Message::Text(payload) => {
                let value = match serde_json::from_str::<Value>(&payload) {
                    Ok(value) => value,
                    Err(_) => {
                        self.send_request_error(
                            socket,
                            None,
                            "urn:ietf:params:jmap:error:notJSON",
                            StatusCode::BAD_REQUEST,
                            "The request did not parse as JSON.",
                        )
                        .await?;
                        return Ok(());
                    }
                };

                let message_type = value
                    .get("@type")
                    .and_then(Value::as_str)
                    .unwrap_or("Request");
                match message_type {
                    "Request" => {
                        let envelope: WebSocketRequestEnvelope = serde_json::from_value(value)?;
                        let response = self
                            .handle_api_request_for_account(
                                account,
                                JmapApiRequest {
                                    using_capabilities: envelope.using_capabilities,
                                    method_calls: envelope.method_calls,
                                },
                            )
                            .await?;
                        let response = WebSocketResponse {
                            type_name: "Response",
                            id: envelope.id,
                            response,
                        };
                        socket
                            .send(Message::Text(serde_json::to_string(&response)?.into()))
                            .await?;
                    }
                    "WebSocketPushEnable" => {
                        let request: WebSocketPushEnable = serde_json::from_value(value)?;
                        subscription.enabled_types = request
                            .data_types
                            .into_iter()
                            .filter(|value| self.supports_push_data_type(value))
                            .collect();
                        subscription.last_type_states.clear();
                        subscription.last_push_state = None;
                        self.enable_push(
                            socket,
                            account.account_id,
                            subscription,
                            request.push_state,
                        )
                        .await?;
                    }
                    "WebSocketPushDisable" => {
                        let _: WebSocketPushDisable = serde_json::from_value(value)?;
                        subscription.enabled_types.clear();
                        subscription.last_type_states.clear();
                        subscription.last_push_state = None;
                    }
                    _ => {
                        self.send_request_error(
                            socket,
                            None,
                            "urn:ietf:params:jmap:error:unknownMethod",
                            StatusCode::BAD_REQUEST,
                            "Unsupported WebSocket JMAP message type.",
                        )
                        .await?;
                    }
                }
            }
            Message::Binary(_) => {
                socket.send(Message::Close(None)).await?;
            }
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await?;
            }
            Message::Close(_) => {
                socket.send(Message::Close(None)).await?;
            }
            Message::Pong(_) => {}
        }
        Ok(())
    }

    async fn enable_push(
        &self,
        socket: &mut WebSocket,
        account_id: Uuid,
        subscription: &mut PushSubscription,
        client_push_state: Option<String>,
    ) -> Result<()> {
        let type_states = self
            .current_push_states(account_id, &subscription.enabled_types)
            .await?;
        let current_push_state = encode_push_state(&type_states)?;
        let should_send = client_push_state
            .as_deref()
            .is_some_and(|push_state| push_state != current_push_state);
        subscription.last_type_states = type_states.clone();
        subscription.last_push_state = Some(current_push_state.clone());
        if should_send {
            self.send_state_change(socket, type_states, current_push_state)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn publish_state_changes(
        &self,
        socket: &mut WebSocket,
        principal_account_id: Uuid,
        subscription: &mut PushSubscription,
        change_set: &CanonicalPushChangeSet,
    ) -> Result<()> {
        let (changed, current_type_states) = self
            .compute_push_changes(principal_account_id, subscription, change_set)
            .await?;

        if changed.is_empty() {
            subscription.last_type_states = current_type_states;
            return Ok(());
        }

        let push_state = encode_push_state(&current_type_states)?;
        subscription.last_type_states = current_type_states;
        subscription.last_push_state = Some(push_state.clone());
        self.send_state_change(socket, changed, push_state).await
    }

    pub(crate) async fn compute_push_changes(
        &self,
        principal_account_id: Uuid,
        subscription: &PushSubscription,
        change_set: &CanonicalPushChangeSet,
    ) -> Result<(
        HashMap<String, HashMap<String, String>>,
        HashMap<String, HashMap<String, String>>,
    )> {
        let mut current_type_states = subscription.last_type_states.clone();
        let mut mail_topology_changed = false;

        if change_set.contains_category(CanonicalChangeCategory::Mail)
            && subscription
                .enabled_types
                .iter()
                .any(|value| self.is_mail_push_type(value))
        {
            let visible_mail_accounts = self
                .store
                .fetch_accessible_mailbox_accounts(principal_account_id)
                .await?
                .into_iter()
                .map(|entry| entry.account_id)
                .collect::<HashSet<_>>();
            let mut tracked_mail_accounts = change_set.accounts_for(CanonicalChangeCategory::Mail);
            tracked_mail_accounts.extend(visible_mail_accounts.iter().copied());
            tracked_mail_accounts.extend(
                subscription
                    .last_type_states
                    .iter()
                    .filter(|(_, states)| {
                        states
                            .keys()
                            .any(|data_type| self.is_mail_push_type(data_type))
                    })
                    .filter_map(|(account_id, _)| Uuid::parse_str(account_id).ok()),
            );

            let previous_visible_mail_accounts = subscription
                .last_type_states
                .iter()
                .filter(|(_, states)| {
                    states
                        .keys()
                        .any(|data_type| self.is_mail_push_type(data_type))
                })
                .filter_map(|(account_id, _)| Uuid::parse_str(account_id).ok())
                .collect::<HashSet<_>>();
            mail_topology_changed = previous_visible_mail_accounts != visible_mail_accounts;

            for account_id in tracked_mail_accounts {
                let account_key = account_id.to_string();
                if visible_mail_accounts.contains(&account_id) {
                    for data_type in subscription
                        .enabled_types
                        .iter()
                        .filter(|value| self.is_mail_push_type(value))
                    {
                        let state = self.object_state(account_id, data_type).await?;
                        current_type_states
                            .entry(account_key.clone())
                            .or_default()
                            .insert(data_type.clone(), state);
                    }
                } else if let Some(states) = current_type_states.get_mut(&account_key) {
                    states.retain(|data_type, _| !self.is_mail_push_type(data_type));
                    if states.is_empty() {
                        current_type_states.remove(&account_key);
                    }
                }
            }
        }

        let principal_key = principal_account_id.to_string();
        for (category, data_types) in [
            (
                CanonicalChangeCategory::Contacts,
                ["AddressBook", "ContactCard"].as_slice(),
            ),
            (
                CanonicalChangeCategory::Calendar,
                ["Calendar", "CalendarEvent"].as_slice(),
            ),
            (
                CanonicalChangeCategory::Tasks,
                ["TaskList", "Task"].as_slice(),
            ),
        ] {
            if !change_set.contains_category(category) {
                continue;
            }
            for data_type in data_types {
                if !subscription.enabled_types.contains(*data_type) {
                    continue;
                }
                let state = self.object_state(principal_account_id, data_type).await?;
                current_type_states
                    .entry(principal_key.clone())
                    .or_default()
                    .insert((*data_type).to_string(), state);
            }
        }

        let mut changed = HashMap::new();
        for (push_account_id, states) in &current_type_states {
            let mut account_changed = HashMap::new();
            for (data_type, state) in states {
                if subscription
                    .last_type_states
                    .get(push_account_id)
                    .and_then(|previous| previous.get(data_type))
                    != Some(state)
                {
                    account_changed.insert(data_type.clone(), state.clone());
                }
            }
            if !account_changed.is_empty() {
                changed.insert(push_account_id.clone(), account_changed);
            }
        }

        if mail_topology_changed {
            let principal_states = current_type_states
                .get(&principal_key)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|(data_type, _)| self.is_mail_push_type(data_type))
                .collect::<HashMap<_, _>>();
            if !principal_states.is_empty() {
                changed
                    .entry(principal_key)
                    .or_default()
                    .extend(principal_states);
            }
        }

        Ok((changed, current_type_states))
    }

    async fn send_request_error(
        &self,
        socket: &mut WebSocket,
        request_id: Option<String>,
        error_type: &str,
        status: StatusCode,
        detail: &str,
    ) -> Result<()> {
        let error = WebSocketRequestError {
            type_name: "RequestError",
            request_id,
            error_type: error_type.to_string(),
            status: status.as_u16(),
            detail: detail.to_string(),
        };
        socket
            .send(Message::Text(serde_json::to_string(&error)?.into()))
            .await?;
        Ok(())
    }

    async fn send_state_change(
        &self,
        socket: &mut WebSocket,
        changed: HashMap<String, HashMap<String, String>>,
        push_state: String,
    ) -> Result<()> {
        let payload = WebSocketStateChange {
            type_name: "StateChange",
            changed,
            push_state: Some(push_state),
        };
        socket
            .send(Message::Text(serde_json::to_string(&payload)?.into()))
            .await?;
        Ok(())
    }

    fn supports_push_data_type(&self, data_type: &str) -> bool {
        matches!(
            data_type,
            "Mailbox"
                | "Email"
                | "Thread"
                | "AddressBook"
                | "ContactCard"
                | "Calendar"
                | "CalendarEvent"
                | "TaskList"
                | "Task"
        )
    }

    fn push_categories(&self, data_types: &HashSet<String>) -> Vec<CanonicalChangeCategory> {
        let mut categories = Vec::new();
        if data_types.iter().any(|value| self.is_mail_push_type(value)) {
            categories.push(CanonicalChangeCategory::Mail);
        }
        if data_types
            .iter()
            .any(|value| matches!(value.as_str(), "AddressBook" | "ContactCard"))
        {
            categories.push(CanonicalChangeCategory::Contacts);
        }
        if data_types
            .iter()
            .any(|value| matches!(value.as_str(), "Calendar" | "CalendarEvent"))
        {
            categories.push(CanonicalChangeCategory::Calendar);
        }
        if data_types
            .iter()
            .any(|value| matches!(value.as_str(), "TaskList" | "Task"))
        {
            categories.push(CanonicalChangeCategory::Tasks);
        }
        categories
    }

    fn is_mail_push_type(&self, data_type: &str) -> bool {
        matches!(data_type, "Mailbox" | "Email" | "Thread")
    }

    pub(crate) async fn current_push_states(
        &self,
        principal_account_id: Uuid,
        data_types: &HashSet<String>,
    ) -> Result<HashMap<String, HashMap<String, String>>> {
        let mut states = HashMap::new();
        if data_types.is_empty() {
            return Ok(states);
        }

        let mailbox_accounts = self
            .store
            .fetch_accessible_mailbox_accounts(principal_account_id)
            .await?;
        for mailbox_account in mailbox_accounts {
            let mut account_states = HashMap::new();
            for data_type in data_types {
                if self.is_mail_push_type(data_type) {
                    let state = self.object_state(mailbox_account.account_id, data_type).await?;
                    account_states.insert(data_type.clone(), state);
                }
            }
            if !account_states.is_empty() {
                states.insert(mailbox_account.account_id.to_string(), account_states);
            }
        }

        for data_type in data_types {
            if self.is_mail_push_type(data_type) {
                continue;
            }
            let state = self.object_state(principal_account_id, data_type).await?;
            states
                .entry(principal_account_id.to_string())
                .or_insert_with(HashMap::new)
                .insert(data_type.clone(), state);
        }
        Ok(states)
    }
}
