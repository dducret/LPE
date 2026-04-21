use anyhow::{anyhow, bail, Result};
use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator};
use lpe_storage::{
    calendar_attendee_labels, mail::parse_rfc822_message, normalize_calendar_email,
    normalize_calendar_participation_status, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, AccessibleContact, AccessibleEvent, AuditEntryInput,
    AuthenticatedAccount, CalendarOrganizerMetadata, CalendarParticipantMetadata,
    CalendarParticipantsMetadata, CanonicalChangeCategory, CanonicalPushChangeSet, ClientTask,
    CollaborationCollection, JmapEmail, JmapEmailAddress, JmapEmailSubmission,
    JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput, JmapMailboxUpdateInput, JmapQuota,
    JmapUploadBlob, MailboxAccountAccess, SavedDraftMessage, SenderIdentity, Storage,
    SubmitMessageInput, SubmittedRecipientInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientTaskInput,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[cfg(test)]
use lpe_storage::{JmapEmailQuery, SubmittedMessage};

mod protocol;
mod store;

use crate::protocol::{
    AddressBookGetArguments, AddressBookQueryArguments, CalendarEventGetArguments,
    CalendarEventQueryArguments, CalendarEventQueryFilter, CalendarEventSetArguments,
    CalendarGetArguments, CalendarQueryArguments, ChangesArguments, ContactCardGetArguments,
    ContactCardQueryArguments, ContactCardQueryFilter, ContactCardSetArguments, DraftMutation,
    EmailAddressInput, EmailCopyArguments, EmailGetArguments, EmailImportArguments,
    EmailQueryArguments, EmailQueryFilter, EmailQuerySort, EmailSetArguments,
    EmailSubmissionGetArguments, EmailSubmissionSetArguments, EntityQuerySort,
    IdentityGetArguments, JmapApiRequest, JmapApiResponse, JmapMethodCall, JmapMethodResponse,
    MailboxCreateInput, MailboxGetArguments, MailboxQueryArguments, MailboxSetArguments,
    MailboxUpdateInput, QueryChangesArguments, QuotaGetArguments, SearchSnippetGetArguments,
    SessionAccount, SessionDocument, TaskGetArguments, TaskListGetArguments, TaskListSetArguments,
    TaskQueryArguments, TaskQueryFilter, TaskQuerySort, TaskSetArguments, ThreadGetArguments,
    ThreadQueryArguments, WebSocketPushDisable, WebSocketPushEnable, WebSocketRequestEnvelope,
    WebSocketRequestError, WebSocketResponse, WebSocketStateChange,
};
use crate::store::{JmapPushListener, JmapStore};

const JMAP_CORE_CAPABILITY: &str = "urn:ietf:params:jmap:core";
const JMAP_MAIL_CAPABILITY: &str = "urn:ietf:params:jmap:mail";
const JMAP_SUBMISSION_CAPABILITY: &str = "urn:ietf:params:jmap:submission";
const JMAP_CONTACTS_CAPABILITY: &str = "urn:ietf:params:jmap:contacts";
const JMAP_CALENDARS_CAPABILITY: &str = "urn:ietf:params:jmap:calendars";
const JMAP_TASKS_CAPABILITY: &str = "urn:ietf:params:jmap:tasks";
const JMAP_WEBSOCKET_CAPABILITY: &str = "urn:ietf:params:jmap:websocket";
const SESSION_STATE: &str = "mvp-3";
const QUERY_STATE_VERSION: &str = "mvp-2";
const STATE_TOKEN_VERSION: &str = "mvp-1";
const MAX_QUERY_LIMIT: u64 = 250;
const DEFAULT_GET_LIMIT: u64 = 100;
type HttpResult<T> = std::result::Result<Json<T>, (StatusCode, String)>;

#[derive(Debug, Serialize, Deserialize)]
struct QueryStateToken {
    version: String,
    kind: String,
    filter: Option<Value>,
    sort: Option<Vec<Value>>,
    ids: Vec<String>,
}

#[derive(Debug, Default)]
struct QueryDiff {
    removed: Vec<String>,
    added: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateToken {
    version: String,
    kind: String,
    entries: Vec<StateEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct StateEntry {
    id: String,
    fingerprint: String,
}

#[derive(Debug, Default)]
struct PushSubscription {
    enabled_types: HashSet<String>,
    last_type_states: HashMap<String, HashMap<String, String>>,
    last_push_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JmapBlobId {
    Upload(Uuid),
    Opaque(String),
}

pub fn router() -> Router<Storage> {
    Router::new()
        .route("/session", get(session_handler))
        .route("/api", post(api_handler))
        .route("/ws", get(websocket_handler))
        .route("/upload/{account_id}", post(upload_handler))
        .route(
            "/download/{account_id}/{blob_id}/{name}",
            get(download_handler),
        )
}

#[derive(Clone)]
pub struct JmapService<S, V = lpe_magika::SystemDetector> {
    store: S,
    validator: Validator<V>,
}

impl<S> JmapService<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            validator: Validator::from_env(),
        }
    }
}

impl<S, V> JmapService<S, V> {
    pub fn new_with_validator(store: S, validator: Validator<V>) -> Self {
        Self { store, validator }
    }
}

async fn session_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> HttpResult<SessionDocument> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let websocket_url = websocket_url(&headers);
    Ok(Json(
        service
            .session_document(authorization.as_deref(), websocket_url.as_deref())
            .await
            .map_err(http_error)?,
    ))
}

async fn api_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<JmapApiRequest>,
) -> HttpResult<JmapApiResponse> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    Ok(Json(
        service
            .handle_api_request(authorization.as_deref(), request)
            .await
            .map_err(http_error)?,
    ))
}

async fn upload_handler(
    State(storage): State<Storage>,
    axum::extract::Path(account_id): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let content_type = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let response = service
        .handle_upload(
            authorization.as_deref(),
            &account_id,
            &content_type,
            body.as_ref(),
        )
        .await
        .map_err(http_error)?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn download_handler(
    State(storage): State<Storage>,
    axum::extract::Path((account_id, blob_id, _name)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let blob = service
        .handle_download(authorization.as_deref(), &account_id, &blob_id)
        .await
        .map_err(http_error)?;
    Ok(([("content-type", blob.media_type.clone())], blob.blob_bytes))
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let account = service
        .authenticate(authorization.as_deref())
        .await
        .map_err(http_error)?;
    Ok(ws.protocols(["jmap"]).on_upgrade(move |socket| async move {
        service.handle_websocket(socket, account).await;
    }))
}

impl<S: JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    async fn requested_account_access(
        &self,
        account: &AuthenticatedAccount,
        requested_account_id: Option<&str>,
    ) -> Result<MailboxAccountAccess> {
        let requested_id = match requested_account_id {
            Some(value) => parse_uuid(value)?,
            None => account.account_id,
        };
        self.store
            .fetch_accessible_mailbox_accounts(account.account_id)
            .await?
            .into_iter()
            .find(|entry| entry.account_id == requested_id)
            .ok_or_else(|| anyhow!("accountId is not accessible"))
    }

    pub async fn session_document(
        &self,
        authorization: Option<&str>,
        websocket_url: Option<&str>,
    ) -> Result<SessionDocument> {
        let account = self.authenticate(authorization).await?;
        let capabilities = session_capabilities(websocket_url.unwrap_or("ws://localhost/jmap/ws"));
        let accessible_accounts = self
            .store
            .fetch_accessible_mailbox_accounts(account.account_id)
            .await?;
        let mut accounts = HashMap::new();
        for accessible in &accessible_accounts {
            accounts.insert(
                accessible.account_id.to_string(),
                SessionAccount {
                    name: accessible.email.clone(),
                    is_personal: accessible.is_owned,
                    is_read_only: mailbox_account_is_read_only(accessible),
                    account_capabilities: capabilities.clone(),
                },
            );
        }

        let mut primary_accounts = HashMap::new();
        let account_id = account.account_id.to_string();
        primary_accounts.insert(JMAP_CORE_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_MAIL_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_SUBMISSION_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_CONTACTS_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_CALENDARS_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_TASKS_CAPABILITY.to_string(), account_id.clone());

        Ok(SessionDocument {
            capabilities,
            accounts,
            primary_accounts,
            username: account.email,
            api_url: "/jmap/api".to_string(),
            download_url: "/jmap/download/{accountId}/{blobId}/{name}".to_string(),
            upload_url: "/jmap/upload/{accountId}".to_string(),
            event_source_url: None,
            state: SESSION_STATE.to_string(),
        })
    }

    pub async fn handle_api_request(
        &self,
        authorization: Option<&str>,
        request: JmapApiRequest,
    ) -> Result<JmapApiResponse> {
        let account = self.authenticate(authorization).await?;
        self.handle_api_request_for_account(&account, request).await
    }

    async fn handle_api_request_for_account(
        &self,
        account: &AuthenticatedAccount,
        request: JmapApiRequest,
    ) -> Result<JmapApiResponse> {
        let _declared_capabilities = request.using_capabilities;
        let mut method_responses = Vec::with_capacity(request.method_calls.len());
        let mut created_ids = HashMap::new();

        for JmapMethodCall(method_name, arguments, call_id) in request.method_calls {
            let response = match method_name.as_str() {
                "Mailbox/get" => self.handle_mailbox_get(&account, arguments).await,
                "Mailbox/query" => self.handle_mailbox_query(&account, arguments).await,
                "Mailbox/queryChanges" => {
                    self.handle_mailbox_query_changes(&account, arguments).await
                }
                "Mailbox/changes" => self.handle_mailbox_changes(&account, arguments).await,
                "Mailbox/set" => {
                    self.handle_mailbox_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Email/query" => self.handle_email_query(&account, arguments).await,
                "Email/queryChanges" => self.handle_email_query_changes(&account, arguments).await,
                "Email/get" => self.handle_email_get(&account, arguments).await,
                "Email/changes" => self.handle_email_changes(&account, arguments).await,
                "Email/set" => {
                    self.handle_email_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Email/copy" => {
                    self.handle_email_copy(&account, arguments, &mut created_ids)
                        .await
                }
                "Email/import" => {
                    self.handle_email_import(&account, arguments, &mut created_ids)
                        .await
                }
                "EmailSubmission/get" => {
                    self.handle_email_submission_get(&account, arguments).await
                }
                "EmailSubmission/set" => {
                    self.handle_email_submission_set(&account, arguments, &mut created_ids)
                        .await
                }
                "AddressBook/get" => self.handle_address_book_get(&account, arguments).await,
                "AddressBook/query" => self.handle_address_book_query(&account, arguments).await,
                "AddressBook/changes" => {
                    self.handle_address_book_changes(&account, arguments).await
                }
                "ContactCard/get" => self.handle_contact_get(&account, arguments).await,
                "ContactCard/query" => self.handle_contact_query(&account, arguments).await,
                "ContactCard/changes" => self.handle_contact_changes(&account, arguments).await,
                "ContactCard/set" => {
                    self.handle_contact_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Calendar/get" => self.handle_calendar_get(&account, arguments).await,
                "Calendar/query" => self.handle_calendar_query(&account, arguments).await,
                "Calendar/changes" => self.handle_calendar_changes(&account, arguments).await,
                "CalendarEvent/get" => self.handle_calendar_event_get(&account, arguments).await,
                "CalendarEvent/query" => {
                    self.handle_calendar_event_query(&account, arguments).await
                }
                "CalendarEvent/changes" => {
                    self.handle_calendar_event_changes(&account, arguments)
                        .await
                }
                "CalendarEvent/set" => {
                    self.handle_calendar_event_set(&account, arguments, &mut created_ids)
                        .await
                }
                "TaskList/get" => self.handle_task_list_get(&account, arguments).await,
                "TaskList/changes" => self.handle_task_list_changes(&account, arguments).await,
                "TaskList/set" => self.handle_task_list_set(&account, arguments).await,
                "Task/get" => self.handle_task_get(&account, arguments).await,
                "Task/query" => self.handle_task_query(&account, arguments).await,
                "Task/queryChanges" => self.handle_task_query_changes(&account, arguments).await,
                "Task/changes" => self.handle_task_changes(&account, arguments).await,
                "Task/set" => {
                    self.handle_task_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Identity/get" => self.handle_identity_get(&account, arguments).await,
                "Thread/query" => self.handle_thread_query(&account, arguments).await,
                "Thread/get" => self.handle_thread_get(&account, arguments).await,
                "Thread/changes" => self.handle_thread_changes(&account, arguments).await,
                "Quota/get" => self.handle_quota_get(&account, arguments).await,
                "SearchSnippet/get" => self.handle_search_snippet_get(&account, arguments).await,
                _ => Ok(method_error("unknownMethod", "method is not supported")),
            };

            let payload = match response {
                Ok(payload) => payload,
                Err(error) => method_error("invalidArguments", &error.to_string()),
            };
            method_responses.push(JmapMethodResponse(method_name, payload, call_id));
        }

        Ok(JmapApiResponse {
            method_responses,
            created_ids,
            session_state: SESSION_STATE.to_string(),
        })
    }

    async fn handle_websocket(&self, mut socket: WebSocket, account: AuthenticatedAccount) {
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

    async fn publish_state_changes(
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

    async fn compute_push_changes(
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

    async fn current_push_states(
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
                    let state = self
                        .object_state(mailbox_account.account_id, data_type)
                        .await?;
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

    async fn object_state(&self, account_id: Uuid, data_type: &str) -> Result<String> {
        let entries = self.object_state_entries(account_id, data_type).await?;
        encode_state(data_type, entries)
    }

    async fn object_state_entries(
        &self,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<Vec<StateEntry>> {
        match data_type {
            "Mailbox" => {
                let mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
                Ok(mailboxes
                    .into_iter()
                    .map(|mailbox| StateEntry {
                        id: mailbox.id.to_string(),
                        fingerprint: format!(
                            "{}|{}|{}|{}|{}",
                            mailbox.role,
                            mailbox.name,
                            mailbox.sort_order,
                            mailbox.total_emails,
                            mailbox.unread_emails
                        ),
                    })
                    .collect())
            }
            "Email" => {
                let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
                let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
                Ok(emails
                    .into_iter()
                    .map(|email| StateEntry {
                        id: email.id.to_string(),
                        fingerprint: email_state_fingerprint(&email),
                    })
                    .collect())
            }
            "Thread" => {
                let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
                let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
                let mut threads: HashMap<Uuid, Vec<String>> = HashMap::new();
                for email in emails {
                    threads.entry(email.thread_id).or_default().push(format!(
                        "{}:{}",
                        email.id,
                        email_state_fingerprint(&email)
                    ));
                }
                let mut entries = threads
                    .into_iter()
                    .map(|(thread_id, mut fingerprints)| {
                        fingerprints.sort();
                        StateEntry {
                            id: thread_id.to_string(),
                            fingerprint: fingerprints.join("|"),
                        }
                    })
                    .collect::<Vec<_>>();
                entries.sort_by(|left, right| left.id.cmp(&right.id));
                Ok(entries)
            }
            "AddressBook" => {
                let collections = self
                    .store
                    .fetch_accessible_contact_collections(account_id)
                    .await?;
                Ok(collections
                    .into_iter()
                    .map(|collection| StateEntry {
                        id: collection.id.clone(),
                        fingerprint: collection_state_fingerprint(&collection),
                    })
                    .collect())
            }
            "ContactCard" => {
                let contacts = self.store.fetch_accessible_contacts(account_id).await?;
                Ok(contacts
                    .into_iter()
                    .map(|contact| StateEntry {
                        id: contact.id.to_string(),
                        fingerprint: contact_state_fingerprint(&contact),
                    })
                    .collect())
            }
            "Calendar" => {
                let collections = self
                    .store
                    .fetch_accessible_calendar_collections(account_id)
                    .await?;
                Ok(collections
                    .into_iter()
                    .map(|collection| StateEntry {
                        id: collection.id.clone(),
                        fingerprint: collection_state_fingerprint(&collection),
                    })
                    .collect())
            }
            "CalendarEvent" => {
                let events = self.store.fetch_accessible_events(account_id).await?;
                Ok(events
                    .into_iter()
                    .map(|event| StateEntry {
                        id: event.id.to_string(),
                        fingerprint: event_state_fingerprint(&event),
                    })
                    .collect())
            }
            "TaskList" => Ok(vec![StateEntry {
                id: default_task_list_id().to_string(),
                fingerprint: default_task_list_state_fingerprint(),
            }]),
            "Task" => {
                let tasks = self.store.fetch_jmap_tasks(account_id).await?;
                Ok(tasks
                    .into_iter()
                    .map(|task| StateEntry {
                        id: task.id.to_string(),
                        fingerprint: task_state_fingerprint(&task),
                    })
                    .collect())
            }
            _ => Ok(Vec::new()),
        }
    }

    async fn handle_mailbox_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: MailboxGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let properties = mailbox_properties(arguments.properties);
        let mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;

        let requested_ids = parse_uuid_list(arguments.ids)?;
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();

        let list = mailboxes
            .iter()
            .filter(|mailbox| requested_ids.is_none() || requested_set.contains(&mailbox.id))
            .map(|mailbox| mailbox_to_value(mailbox, &account_access, &properties))
            .collect::<Vec<_>>();

        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !mailboxes.iter().any(|mailbox| mailbox.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Mailbox").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_mailbox_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: MailboxQueryArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let mut mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        mailboxes.sort_by_key(|mailbox| (mailbox.sort_order, mailbox.name.to_lowercase()));
        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let all_ids = mailboxes
            .iter()
            .map(|mailbox| mailbox.id.to_string())
            .collect::<Vec<_>>();
        let ids = all_ids
            .iter()
            .skip(position)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let query_state = encode_query_state("Mailbox/query", None, None, all_ids)?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": query_state,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": mailboxes.len(),
        }))
    }

    async fn handle_mailbox_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let mut mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        mailboxes.sort_by_key(|mailbox| (mailbox.sort_order, mailbox.name.to_lowercase()));
        let current_ids = mailboxes
            .into_iter()
            .map(|mailbox| mailbox.id.to_string())
            .collect::<Vec<_>>();
        let total = current_ids.len() as u64;
        query_changes_response(
            account_id,
            "Mailbox/query",
            arguments.since_query_state,
            arguments.filter,
            arguments.sort.map(|sort| sort.into_iter().collect()),
            current_ids,
            total,
            arguments.max_changes,
        )
    }

    async fn handle_mailbox_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let entries = self.object_state_entries(account_id, "Mailbox").await?;
        Ok(changes_response(
            account_id,
            "Mailbox",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    async fn handle_mailbox_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: MailboxSetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let old_state = self.object_state(account_id, "Mailbox").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_mailbox_create(value) {
                    Ok(input) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-create".to_string(),
                            subject: creation_id.clone(),
                        };
                        match self
                            .store
                            .create_jmap_mailbox(
                                JmapMailboxCreateInput {
                                    account_id,
                                    name: input.name,
                                    sort_order: input.sort_order,
                                },
                                audit,
                            )
                            .await
                        {
                            Ok(mailbox) => {
                                created_ids.insert(creation_id.clone(), mailbox.id.to_string());
                                created.insert(creation_id, json!({"id": mailbox.id.to_string()}));
                            }
                            Err(error) => {
                                not_created.insert(creation_id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match parse_uuid(&id).and_then(|mailbox_id| {
                    parse_mailbox_update(value).map(|input| (mailbox_id, input))
                }) {
                    Ok((mailbox_id, input)) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-update".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .update_jmap_mailbox(
                                JmapMailboxUpdateInput {
                                    account_id,
                                    mailbox_id,
                                    name: input.name,
                                    sort_order: input.sort_order,
                                },
                                audit,
                            )
                            .await
                        {
                            Ok(_) => {
                                updated.insert(id, Value::Object(Map::new()));
                            }
                            Err(error) => {
                                not_updated.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(mailbox_id) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-destroy".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .destroy_jmap_mailbox(account_id, mailbox_id, audit)
                            .await
                        {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        let new_state = self.object_state(account_id, "Mailbox").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_address_book_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: AddressBookGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = address_book_properties(arguments.properties);
        let requested_ids = arguments.ids.unwrap_or_default();
        let collections = self
            .store
            .fetch_accessible_contact_collections(account_id)
            .await?;
        let list = collections
            .iter()
            .filter(|collection| requested_ids.is_empty() || requested_ids.contains(&collection.id))
            .map(|collection| address_book_to_value(collection, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .into_iter()
            .filter(|id| !collections.iter().any(|collection| collection.id == *id))
            .map(Value::String)
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "AddressBook").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_address_book_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: AddressBookQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let collections = self
            .store
            .fetch_accessible_contact_collections(account_id)
            .await?;
        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = collections
            .iter()
            .map(|collection| collection.id.clone())
            .skip(position)
            .take(limit)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": collections.len(),
        }))
    }

    async fn handle_address_book_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let collections = self
            .store
            .fetch_accessible_contact_collections(account_id)
            .await?;
        let entries = collections
            .into_iter()
            .map(|collection| StateEntry {
                id: collection.id.clone(),
                fingerprint: collection_state_fingerprint(&collection),
            })
            .collect::<Vec<_>>();
        Ok(changes_response(
            account_id,
            "AddressBook",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    async fn handle_contact_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ContactCardGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = contact_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let contacts = if let Some(ids) = requested_ids.as_ref() {
            self.store
                .fetch_accessible_contacts_by_ids(account_id, &ids)
                .await?
        } else {
            self.store.fetch_accessible_contacts(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();

        let list = contacts
            .iter()
            .filter(|contact| requested_ids.is_none() || requested_set.contains(&contact.id))
            .map(|contact| contact_to_value(contact, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !contacts.iter().any(|contact| contact.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "ContactCard").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_contact_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ContactCardQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_entity_sort(arguments.sort.as_deref(), "name", true)?;
        validate_contact_filter(arguments.filter.as_ref())?;

        let mut contacts = self.store.fetch_accessible_contacts(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            contacts.retain(|contact| contact_matches_filter(contact, filter));
        }
        contacts.sort_by_key(|contact| contact.name.to_lowercase());

        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = contacts
            .iter()
            .skip(position)
            .take(limit)
            .map(|contact| contact.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": contacts.len(),
        }))
    }

    async fn handle_contact_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self.object_state_entries(account_id, "ContactCard").await?;
        Ok(changes_response(
            account_id,
            "ContactCard",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    async fn handle_contact_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: ContactCardSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "ContactCard").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_contact_input(None, account_id, value) {
                    Ok((collection_id, input)) => match self
                        .store
                        .create_accessible_contact(account_id, collection_id.as_deref(), input)
                        .await
                    {
                        Ok(contact) => {
                            created_ids.insert(creation_id.clone(), contact.id.to_string());
                            created.insert(creation_id, json!({ "id": contact.id.to_string() }));
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match parse_uuid(&id).and_then(|contact_id| {
                    parse_contact_input(Some(contact_id), account_id, value)
                        .map(|(_, input)| (contact_id, input))
                }) {
                    Ok((contact_id, input)) => match self
                        .store
                        .update_accessible_contact(account_id, contact_id, input)
                        .await
                    {
                        Ok(_) => {
                            updated.insert(id, Value::Object(Map::new()));
                        }
                        Err(error) => {
                            not_updated.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(contact_id) => match self
                        .store
                        .delete_accessible_contact(account_id, contact_id)
                        .await
                    {
                        Ok(()) => destroyed.push(Value::String(id)),
                        Err(error) => {
                            not_destroyed.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        let new_state = self.object_state(account_id, "ContactCard").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_calendar_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = calendar_properties(arguments.properties);
        let requested_ids = arguments.ids.unwrap_or_default();
        let collections = self
            .store
            .fetch_accessible_calendar_collections(account_id)
            .await?;
        let list = collections
            .iter()
            .filter(|collection| requested_ids.is_empty() || requested_ids.contains(&collection.id))
            .map(|collection| calendar_to_value(collection, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .into_iter()
            .filter(|id| !collections.iter().any(|collection| collection.id == *id))
            .map(Value::String)
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Calendar").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_calendar_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let collections = self
            .store
            .fetch_accessible_calendar_collections(account_id)
            .await?;
        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = collections
            .iter()
            .map(|collection| collection.id.clone())
            .skip(position)
            .take(limit)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": collections.len(),
        }))
    }

    async fn handle_calendar_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let collections = self
            .store
            .fetch_accessible_calendar_collections(account_id)
            .await?;
        let entries = collections
            .into_iter()
            .map(|collection| StateEntry {
                id: collection.id.clone(),
                fingerprint: collection_state_fingerprint(&collection),
            })
            .collect::<Vec<_>>();
        Ok(changes_response(
            account_id,
            "Calendar",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    async fn handle_calendar_event_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarEventGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = calendar_event_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let events = if let Some(ids) = requested_ids.as_ref() {
            self.store
                .fetch_accessible_events_by_ids(account_id, ids)
                .await?
        } else {
            self.store.fetch_accessible_events(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();
        let list = events
            .iter()
            .filter(|event| requested_ids.is_none() || requested_set.contains(&event.id))
            .map(|event| calendar_event_to_value(event, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !events.iter().any(|event| event.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "CalendarEvent").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_calendar_event_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarEventQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_entity_sort(arguments.sort.as_deref(), "start", true)?;
        validate_calendar_event_filter(arguments.filter.as_ref())?;

        let mut events = self.store.fetch_accessible_events(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            events.retain(|event| event_matches_filter(event, filter));
        }
        events.sort_by_key(calendar_event_sort_key);

        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = events
            .iter()
            .skip(position)
            .take(limit)
            .map(|event| event.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": events.len(),
        }))
    }

    async fn handle_calendar_event_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self
            .object_state_entries(account_id, "CalendarEvent")
            .await?;
        Ok(changes_response(
            account_id,
            "CalendarEvent",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    async fn handle_calendar_event_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: CalendarEventSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "CalendarEvent").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_calendar_event_input(None, account_id, value) {
                    Ok((collection_id, input)) => match self
                        .store
                        .create_accessible_event(account_id, collection_id.as_deref(), input)
                        .await
                    {
                        Ok(event) => {
                            created_ids.insert(creation_id.clone(), event.id.to_string());
                            created.insert(creation_id, json!({ "id": event.id.to_string() }));
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match parse_uuid(&id).and_then(|event_id| {
                    parse_calendar_event_input(Some(event_id), account_id, value)
                        .map(|(_, input)| (event_id, input))
                }) {
                    Ok((event_id, input)) => match self
                        .store
                        .update_accessible_event(account_id, event_id, input)
                        .await
                    {
                        Ok(_) => {
                            updated.insert(id, Value::Object(Map::new()));
                        }
                        Err(error) => {
                            not_updated.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(event_id) => {
                        match self
                            .store
                            .delete_accessible_event(account_id, event_id)
                            .await
                        {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        let new_state = self.object_state(account_id, "CalendarEvent").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_task_list_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: TaskListGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = task_list_properties(arguments.properties);
        let requested_ids = arguments.ids.unwrap_or_default();
        let list = if requested_ids.is_empty()
            || requested_ids.iter().any(|id| id == default_task_list_id())
        {
            vec![task_list_to_value(&properties)]
        } else {
            Vec::new()
        };
        let not_found = requested_ids
            .into_iter()
            .filter(|id| id != default_task_list_id())
            .map(Value::String)
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "TaskList").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_task_list_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self.object_state_entries(account_id, "TaskList").await?;
        Ok(changes_response(
            account_id,
            "TaskList",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    async fn handle_task_list_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: TaskListSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "TaskList").await?;
        let mut not_created = Map::new();
        let mut not_updated = Map::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, _) in create {
                not_created.insert(
                    creation_id,
                    method_error("forbidden", "TaskList/set is not supported in the MVP"),
                );
            }
        }
        if let Some(update) = arguments.update {
            for (id, _) in update {
                not_updated.insert(
                    id,
                    method_error("forbidden", "TaskList/set is not supported in the MVP"),
                );
            }
        }
        if let Some(ids) = arguments.destroy {
            for id in ids {
                not_destroyed.insert(
                    id,
                    method_error("forbidden", "TaskList/set is not supported in the MVP"),
                );
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state.clone(),
            "newState": old_state,
            "created": Value::Object(Map::new()),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(Map::new()),
            "notUpdated": Value::Object(not_updated),
            "destroyed": Vec::<String>::new(),
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_task_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: TaskGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = task_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let tasks = if let Some(ids) = requested_ids.as_ref() {
            self.store.fetch_jmap_tasks_by_ids(account_id, ids).await?
        } else {
            self.store.fetch_jmap_tasks(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();
        let list = tasks
            .iter()
            .filter(|task| requested_ids.is_none() || requested_set.contains(&task.id))
            .map(|task| task_to_value(task, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !tasks.iter().any(|task| task.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Task").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_task_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: TaskQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_task_sort(arguments.sort.as_deref())?;
        validate_task_filter(arguments.filter.as_ref())?;

        let mut tasks = self.store.fetch_jmap_tasks(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            tasks.retain(|task| task_matches_filter(task, filter));
        }
        tasks.sort_by_key(task_sort_key);

        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = tasks
            .iter()
            .skip(position)
            .take(limit)
            .map(|task| task.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": encode_query_state(
                "Task",
                arguments.filter.map(|filter| serde_json::to_value(filter)).transpose()?,
                arguments
                    .sort
                    .map(|sort| {
                        sort.into_iter()
                            .map(serde_json::to_value)
                            .collect::<std::result::Result<Vec<_>, _>>()
                    })
                    .transpose()?,
                tasks.iter().map(|task| task.id.to_string()).collect(),
            )?,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": tasks.len(),
        }))
    }

    async fn handle_task_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<TaskQueryFilter, TaskQuerySort> =
            serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_task_sort(arguments.sort.as_deref())?;
        validate_task_filter(arguments.filter.as_ref())?;

        let mut tasks = self.store.fetch_jmap_tasks(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            tasks.retain(|task| task_matches_filter(task, filter));
        }
        tasks.sort_by_key(task_sort_key);

        query_changes_response(
            account_id,
            "Task",
            arguments.since_query_state,
            arguments.filter.map(serde_json::to_value).transpose()?,
            arguments
                .sort
                .map(|sort| {
                    sort.into_iter()
                        .map(serde_json::to_value)
                        .collect::<std::result::Result<Vec<_>, _>>()
                })
                .transpose()?,
            tasks.iter().map(|task| task.id.to_string()).collect(),
            tasks.len() as u64,
            arguments.max_changes,
        )
    }

    async fn handle_task_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self.object_state_entries(account_id, "Task").await?;
        Ok(changes_response(
            account_id,
            "Task",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    async fn handle_task_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: TaskSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "Task").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_task_input(None, account_id, value) {
                    Ok(input) => match self.store.upsert_jmap_task(input).await {
                        Ok(task) => {
                            created_ids.insert(creation_id.clone(), task.id.to_string());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": task.id.to_string(),
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match parse_uuid(&id)
                    .and_then(|task_id| parse_task_input(Some(task_id), account_id, value))
                {
                    Ok(input) => match self.store.upsert_jmap_task(input).await {
                        Ok(_) => {
                            updated.insert(id, Value::Object(Map::new()));
                        }
                        Err(error) => {
                            not_updated.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(task_id) => match self.store.delete_jmap_task(account_id, task_id).await {
                        Ok(()) => destroyed.push(Value::String(id)),
                        Err(error) => {
                            not_destroyed.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        let new_state = self.object_state(account_id, "Task").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_email_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailQueryArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        validate_query_sort(arguments.sort.as_deref())?;

        let mailbox_id = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.in_mailbox.as_deref())
            .map(|value| parse_uuid(&value))
            .transpose()?;
        let search_text = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.text.as_deref());
        let position = arguments.position.unwrap_or(0);
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT);
        let query = self
            .store
            .query_jmap_email_ids(account_id, mailbox_id, search_text, position, limit)
            .await?;
        let full_ids = self
            .resolve_full_email_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        let ids = query
            .ids
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>();
        let query_state = encode_query_state(
            "Email/query",
            arguments
                .filter
                .as_ref()
                .map(serialize_email_query_filter)
                .transpose()?,
            arguments
                .sort
                .as_ref()
                .map(|sort| serialize_email_query_sort(sort))
                .transpose()?,
            full_ids,
        )?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": query_state,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": query.total,
        }))
    }

    async fn handle_email_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<EmailQueryFilter, EmailQuerySort> =
            serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        validate_query_sort(arguments.sort.as_deref())?;

        let mailbox_id = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.in_mailbox.as_deref())
            .map(parse_uuid)
            .transpose()?;
        let search_text = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.text.as_deref());
        let query = self
            .store
            .query_jmap_email_ids(account_id, mailbox_id, search_text, 0, MAX_QUERY_LIMIT)
            .await?;
        let current_ids = self
            .resolve_full_email_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        query_changes_response(
            account_id,
            "Email/query",
            arguments.since_query_state,
            arguments
                .filter
                .as_ref()
                .map(serialize_email_query_filter)
                .transpose()?,
            arguments
                .sort
                .as_ref()
                .map(|sort| serialize_email_query_sort(sort))
                .transpose()?,
            current_ids,
            query.total,
            arguments.max_changes,
        )
    }

    async fn handle_email_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let properties = email_properties(arguments.properties);

        let ids = match parse_uuid_list(arguments.ids)? {
            Some(ids) => ids,
            None => {
                self.store
                    .query_jmap_email_ids(account_id, None, None, 0, DEFAULT_GET_LIMIT)
                    .await?
                    .ids
            }
        };

        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let not_found = ids
            .iter()
            .filter(|id| !emails.iter().any(|email| email.id == **id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Email").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": emails.iter().map(|email| email_to_value(email, &properties)).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    async fn handle_email_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let entries = self.object_state_entries(account_id, "Email").await?;
        Ok(changes_response(
            account_id,
            "Email",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    async fn handle_email_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailCopyArguments = serde_json::from_value(arguments)?;
        let from_account_access = self
            .requested_account_access(account, Some(&arguments.from_account_id))
            .await?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let from_account_id = from_account_access.account_id;
        let account_id = account_access.account_id;
        if from_account_id != account_id {
            bail!("cross-account Email/copy is not supported");
        }

        let old_state = self.object_state(account_id, "Email").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        for (creation_id, value) in arguments.create {
            match parse_email_copy(value, created_ids) {
                Ok((email_id, mailbox_id)) => {
                    let audit = AuditEntryInput {
                        actor: account.email.clone(),
                        action: "jmap-email-copy".to_string(),
                        subject: creation_id.clone(),
                    };
                    match self
                        .store
                        .copy_jmap_email(account_id, email_id, mailbox_id, audit)
                        .await
                    {
                        Ok(email) => {
                            created_ids.insert(creation_id.clone(), email.id.to_string());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": email.id.to_string(),
                                    "blobId": blob_id_for_message(&email),
                                    "threadId": email.thread_id.to_string(),
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_created.insert(creation_id, set_error(&error.to_string()));
                }
            }
        }

        let new_state = self.object_state(account_id, "Email").await?;
        Ok(json!({
            "fromAccountId": from_account_id.to_string(),
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    async fn handle_email_import(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailImportArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let old_state = self.object_state(account_id, "Email").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();

        for (creation_id, value) in arguments.emails {
            match self
                .parse_email_import(account, &account_access, value, created_ids)
                .await
            {
                Ok(input) => {
                    let audit = AuditEntryInput {
                        actor: account.email.clone(),
                        action: "jmap-email-import".to_string(),
                        subject: creation_id.clone(),
                    };
                    match self.store.import_jmap_email(input, audit).await {
                        Ok(email) => {
                            created_ids.insert(creation_id.clone(), email.id.to_string());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": email.id.to_string(),
                                    "blobId": blob_id_for_message(&email),
                                    "threadId": email.thread_id.to_string(),
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_created.insert(creation_id, set_error(&error.to_string()));
                }
            }
        }

        let new_state = self.object_state(account_id, "Email").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    async fn handle_email_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailSetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let old_state = self.object_state(account_id, "Email").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match self
                    .create_draft(account, &account_access, value, creation_id.as_str())
                    .await
                {
                    Ok(saved) => {
                        created_ids.insert(creation_id.clone(), saved.message_id.to_string());
                        created.insert(
                            creation_id,
                            json!({
                                "id": saved.message_id.to_string(),
                                "blobId": format!("draft:{}", saved.message_id),
                            }),
                        );
                    }
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match self
                    .update_draft(account, &account_access, &id, value)
                    .await
                {
                    Ok(_) => {
                        updated.insert(id, Value::Object(Map::new()));
                    }
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(message_id) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-email-draft-delete".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .delete_draft_message(account_id, message_id, audit)
                            .await
                        {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        let new_state = self.object_state(account_id, "Email").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_email_submission_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailSubmissionSetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        if !mailbox_account_may_submit(&account_access) {
            bail!("sender delegation is required to submit from a delegated mailbox");
        }
        let old_state = self.object_state(account_id, "Email").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_submission_email_id(&value, created_ids)? {
                    Some(email_id) => {
                        let message_id = parse_uuid(&email_id)?;
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-email-submit".to_string(),
                            subject: email_id.clone(),
                        };
                        match self
                            .store
                            .submit_draft_message(account_id, message_id, "jmap", audit)
                            .await
                        {
                            Ok(result) => {
                                created_ids.insert(
                                    creation_id.clone(),
                                    result.outbound_queue_id.to_string(),
                                );
                                created.insert(
                                    creation_id,
                                    json!({
                                        "id": result.outbound_queue_id.to_string(),
                                        "emailId": result.message_id.to_string(),
                                        "threadId": result.thread_id.to_string(),
                                        "undoStatus": "final",
                                    }),
                                );
                            }
                            Err(error) => {
                                not_created.insert(creation_id, set_error(&error.to_string()));
                            }
                        }
                    }
                    None => {
                        not_created.insert(
                            creation_id,
                            method_error("invalidArguments", "emailId is required"),
                        );
                    }
                }
            }
        }

        let new_state = self.object_state(account_id, "Email").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    async fn handle_email_submission_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailSubmissionGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let ids = parse_uuid_list(arguments.ids)?;
        let properties = email_submission_properties(arguments.properties);
        let ids_ref = ids.as_deref().unwrap_or(&[]);
        let submissions = self
            .store
            .fetch_jmap_email_submissions(account_id, ids_ref)
            .await?;
        let not_found = ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !submissions.iter().any(|submission| submission.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Email").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": submissions.iter().map(|submission| email_submission_to_value(submission, &properties)).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    async fn handle_identity_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: IdentityGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let properties = identity_properties(arguments.properties);
        let identities = self
            .store
            .fetch_sender_identities(account.account_id, account_id)
            .await?;
        let ids = arguments.ids.unwrap_or_else(|| {
            identities
                .iter()
                .map(|identity| identity.id.clone())
                .collect::<Vec<_>>()
        });
        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            if let Some(identity) = identities.iter().find(|identity| identity.id == id) {
                list.push(identity_to_value(identity, &properties));
            } else {
                not_found.push(Value::String(id));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_thread_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ThreadQueryArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        validate_query_sort(arguments.sort.as_deref())?;

        let mailbox_id = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.in_mailbox.as_deref())
            .map(parse_uuid)
            .transpose()?;
        let search_text = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.text.as_deref());
        let position = arguments.position.unwrap_or(0);
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT);
        let query = self
            .store
            .query_jmap_thread_ids(account_id, mailbox_id, search_text, position, limit)
            .await?;
        let full_ids = self
            .resolve_full_thread_query_ids(account_id, mailbox_id, search_text, &query)
            .await?;
        let ids = query
            .ids
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>();
        let query_state = encode_query_state(
            "Thread/query",
            arguments
                .filter
                .as_ref()
                .map(serialize_email_query_filter)
                .transpose()?,
            arguments
                .sort
                .as_ref()
                .map(|sort| serialize_email_query_sort(sort))
                .transpose()?,
            full_ids,
        )?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": query_state,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": query.total,
        }))
    }

    async fn handle_thread_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ThreadGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let properties = thread_properties(arguments.properties);
        let all_email_ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
        let emails = self
            .store
            .fetch_jmap_emails(account_id, &all_email_ids)
            .await?;
        let ids = arguments.ids.unwrap_or_else(|| {
            emails
                .iter()
                .map(|email| email.thread_id.to_string())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        });

        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            let thread_id = parse_uuid(&id)?;
            let thread_emails = emails
                .iter()
                .filter(|email| email.thread_id == thread_id)
                .map(|email| email.id.to_string())
                .collect::<Vec<_>>();
            if thread_emails.is_empty() {
                not_found.push(Value::String(id));
            } else {
                list.push(thread_to_value(thread_id, thread_emails, &properties));
            }
        }
        let state = self.object_state(account_id, "Thread").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_thread_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let entries = self.object_state_entries(account_id, "Thread").await?;
        Ok(changes_response(
            account_id,
            "Thread",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    async fn handle_quota_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QuotaGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let quota = self.store.fetch_jmap_quota(account_id).await?;
        let ids = arguments.ids.unwrap_or_else(|| vec![quota.id.clone()]);
        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            if id == quota.id {
                list.push(quota_to_value(&quota));
            } else {
                not_found.push(Value::String(id));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_search_snippet_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: SearchSnippetGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let ids = parse_uuid_list(Some(arguments.email_ids))?.unwrap_or_default();
        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let not_found = ids
            .iter()
            .filter(|id| !emails.iter().any(|email| email.id == **id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "list": emails.iter().map(search_snippet_to_value).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    async fn handle_upload(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        media_type: &str,
        body: &[u8],
    ) -> Result<Value> {
        let account = self.authenticate(authorization).await?;
        let requested_account = self
            .requested_account_access(&account, Some(account_id))
            .await?;
        let requested_account_id = requested_account.account_id;
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapUpload,
                declared_mime: Some(media_type.to_string()),
                filename: None,
                expected_kind: ExpectedKind::Any,
            },
            body,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP upload blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let blob = self
            .store
            .save_jmap_upload_blob(requested_account_id, media_type, body)
            .await?;

        Ok(json!({
            "accountId": requested_account_id.to_string(),
            "blobId": blob.id.to_string(),
            "type": blob.media_type,
            "size": blob.octet_size,
        }))
    }

    async fn handle_download(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        blob_id: &str,
    ) -> Result<JmapUploadBlob> {
        let account = self.authenticate(authorization).await?;
        let requested_account = self
            .requested_account_access(&account, Some(account_id))
            .await?;
        let requested_account_id = requested_account.account_id;
        let blob_id = parse_upload_blob_id(blob_id)?;
        self.store
            .fetch_jmap_upload_blob(requested_account_id, blob_id)
            .await?
            .ok_or_else(|| anyhow!("blob not found"))
    }

    async fn resolve_full_email_query_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        query: &lpe_storage::JmapEmailQuery,
    ) -> Result<Vec<String>> {
        let ids = if query.total > query.ids.len() as u64 {
            self.store
                .query_jmap_email_ids(
                    account_id,
                    mailbox_id,
                    search_text,
                    0,
                    full_query_limit(query.total),
                )
                .await?
                .ids
        } else {
            query.ids.clone()
        };

        Ok(ids.into_iter().map(|id| id.to_string()).collect())
    }

    async fn resolve_full_thread_query_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        query: &lpe_storage::JmapThreadQuery,
    ) -> Result<Vec<String>> {
        let ids = if query.total > query.ids.len() as u64 {
            self.store
                .query_jmap_thread_ids(
                    account_id,
                    mailbox_id,
                    search_text,
                    0,
                    full_query_limit(query.total),
                )
                .await?
                .ids
        } else {
            query.ids.clone()
        };

        Ok(ids.into_iter().map(|id| id.to_string()).collect())
    }

    async fn create_draft(
        &self,
        account: &AuthenticatedAccount,
        account_access: &MailboxAccountAccess,
        value: Value,
        creation_id: &str,
    ) -> Result<SavedDraftMessage> {
        let mutation = parse_draft_mutation(value)?;
        let (from, sender) =
            select_from_addresses(mutation.from, mutation.sender, account, account_access)?;
        let audit = AuditEntryInput {
            actor: account.email.clone(),
            action: "jmap-email-draft-create".to_string(),
            subject: creation_id.to_string(),
        };
        self.store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: account_access.account_id,
                    submitted_by_account_id: account.account_id,
                    source: "jmap".to_string(),
                    from_display: from.name,
                    from_address: from.email,
                    sender_display: sender.as_ref().and_then(|value| value.name.clone()),
                    sender_address: sender.map(|value| value.email),
                    to: map_recipients(mutation.to.unwrap_or_default())?,
                    cc: map_recipients(mutation.cc.unwrap_or_default())?,
                    bcc: map_recipients(mutation.bcc.unwrap_or_default())?,
                    subject: mutation.subject.unwrap_or_default(),
                    body_text: mutation.text_body.unwrap_or_default(),
                    body_html_sanitized: mutation.html_body.unwrap_or(None),
                    internet_message_id: None,
                    mime_blob_ref: None,
                    size_octets: 0,
                    unread: Some(mutation.unread.unwrap_or(false)),
                    flagged: Some(mutation.flagged.unwrap_or(false)),
                    attachments: Vec::new(),
                },
                audit,
            )
            .await
    }

    async fn update_draft(
        &self,
        account: &AuthenticatedAccount,
        account_access: &MailboxAccountAccess,
        id: &str,
        value: Value,
    ) -> Result<SavedDraftMessage> {
        let message_id = parse_uuid(id)?;
        let existing = self
            .store
            .fetch_jmap_draft(account_access.account_id, message_id)
            .await?
            .ok_or_else(|| anyhow!("draft not found"))?;
        let mutation = parse_draft_mutation(value)?;
        let (from, sender) =
            select_from_addresses(mutation.from, mutation.sender, account, account_access)?;
        let audit = AuditEntryInput {
            actor: account.email.clone(),
            action: "jmap-email-draft-update".to_string(),
            subject: id.to_string(),
        };

        self.store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: Some(message_id),
                    account_id: account_access.account_id,
                    submitted_by_account_id: account.account_id,
                    source: "jmap".to_string(),
                    from_display: from.name.or(existing.from_display.clone()),
                    from_address: if from.email.trim().is_empty() {
                        existing.from_address
                    } else {
                        from.email
                    },
                    sender_display: sender
                        .as_ref()
                        .and_then(|value| value.name.clone())
                        .or(existing.sender_display),
                    sender_address: sender.map(|value| value.email).or(existing.sender_address),
                    to: mutation
                        .to
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.to)),
                    cc: mutation
                        .cc
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.cc)),
                    bcc: mutation
                        .bcc
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.bcc)),
                    subject: mutation.subject.unwrap_or(existing.subject),
                    body_text: mutation.text_body.unwrap_or(existing.body_text),
                    body_html_sanitized: mutation.html_body.unwrap_or(existing.body_html_sanitized),
                    internet_message_id: existing.internet_message_id,
                    mime_blob_ref: None,
                    size_octets: existing.size_octets,
                    unread: Some(mutation.unread.unwrap_or(existing.unread)),
                    flagged: Some(mutation.flagged.unwrap_or(existing.flagged)),
                    attachments: Vec::new(),
                },
                audit,
            )
            .await
    }

    async fn parse_email_import(
        &self,
        account: &AuthenticatedAccount,
        account_access: &MailboxAccountAccess,
        value: Value,
        created_ids: &HashMap<String, String>,
    ) -> Result<JmapImportedEmailInput> {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("import arguments must be an object"))?;
        let blob_id = object
            .get("blobId")
            .and_then(Value::as_str)
            .map(|value| resolve_creation_reference(value, created_ids))
            .ok_or_else(|| anyhow!("blobId is required"))?;
        let blob_id = parse_upload_blob_id(&blob_id)?;
        let mailbox_ids = object
            .get("mailboxIds")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("mailboxIds is required"))?;
        let target_mailbox_id = mailbox_ids
            .iter()
            .find(|(_, included)| included.as_bool().unwrap_or(false))
            .map(|(mailbox_id, _)| parse_uuid(mailbox_id))
            .transpose()?
            .ok_or_else(|| anyhow!("one target mailboxId is required"))?;
        let blob = self
            .store
            .fetch_jmap_upload_blob(account_access.account_id, blob_id)
            .await?
            .ok_or_else(|| anyhow!("uploaded blob not found"))?;
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapEmailImport,
                declared_mime: Some(blob.media_type.clone()),
                filename: None,
                expected_kind: ExpectedKind::Rfc822Message,
            },
            &blob.blob_bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP email import blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let parsed = parse_rfc822_message(&blob.blob_bytes)?;
        self.validate_imported_attachments(&parsed.attachments)?;

        Ok(JmapImportedEmailInput {
            account_id: account_access.account_id,
            submitted_by_account_id: account.account_id,
            mailbox_id: target_mailbox_id,
            source: "jmap-import".to_string(),
            from_display: parsed
                .from
                .as_ref()
                .and_then(|from| from.display_name.clone())
                .or(Some(account_access.display_name.clone())),
            from_address: parsed
                .from
                .map(|from| from.email)
                .unwrap_or_else(|| account_access.email.clone()),
            sender_display: None,
            sender_address: None,
            to: map_parsed_recipients(parsed.to),
            cc: map_parsed_recipients(parsed.cc),
            bcc: Vec::new(),
            subject: parsed.subject,
            body_text: parsed.body_text,
            body_html_sanitized: parsed.body_html_sanitized,
            internet_message_id: parsed.message_id,
            mime_blob_ref: format!("upload:{}", blob.id),
            size_octets: blob.octet_size as i64,
            received_at: None,
            attachments: parsed.attachments,
        })
    }

    fn validate_imported_attachments(
        &self,
        attachments: &[lpe_storage::AttachmentUploadInput],
    ) -> Result<()> {
        for attachment in attachments {
            let outcome = self.validator.validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::AttachmentParsing,
                    declared_mime: Some(attachment.media_type.clone()),
                    filename: Some(attachment.file_name.clone()),
                    expected_kind: expected_attachment_kind(
                        attachment.media_type.as_str(),
                        attachment.file_name.as_str(),
                    ),
                },
                &attachment.blob_bytes,
            )?;
            if outcome.policy_decision != PolicyDecision::Accept {
                bail!(
                    "JMAP email import attachment '{}' blocked by Magika validation: {}",
                    attachment.file_name,
                    outcome.reason
                );
            }
        }

        Ok(())
    }

    async fn authenticate(&self, authorization: Option<&str>) -> Result<AuthenticatedAccount> {
        let token = bearer_token(authorization).ok_or_else(|| anyhow!("missing bearer token"))?;
        self.store
            .fetch_account_session(token)
            .await?
            .ok_or_else(|| anyhow!("invalid or expired account session"))
    }
}

fn authorization_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn websocket_url(headers: &HeaderMap) -> Option<String> {
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get("host"))
        .and_then(|value| value.to_str().ok())?;
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .map(|value| match value {
            "https" => "wss",
            "http" => "ws",
            other if other.starts_with("ws") => other,
            _ => "ws",
        })
        .unwrap_or("ws");
    Some(format!("{scheme}://{host}/jmap/ws"))
}

fn bearer_token(authorization: Option<&str>) -> Option<&str> {
    authorization
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn http_error(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    let status = if message.contains("bearer token") || message.contains("expired account session")
    {
        StatusCode::UNAUTHORIZED
    } else if message.contains("Magika command")
        || message.contains("spawn Magika")
        || message.contains("Magika stdin")
    {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, message)
}

fn session_capabilities(websocket_url: &str) -> HashMap<String, Value> {
    HashMap::from([
        (
            JMAP_CORE_CAPABILITY.to_string(),
            json!({
                "maxSizeUpload": 0,
                "maxCallsInRequest": 16,
                "maxConcurrentUpload": 0,
                "maxObjectsInGet": 250,
                "maxObjectsInSet": 128,
                "collationAlgorithms": ["i;ascii-casemap"],
            }),
        ),
        (
            JMAP_MAIL_CAPABILITY.to_string(),
            json!({
                "maxMailboxesPerEmail": 1,
                "maxMailboxDepth": 1,
                "emailQuerySortOptions": ["receivedAt"],
            }),
        ),
        (
            JMAP_SUBMISSION_CAPABILITY.to_string(),
            json!({
                "maxDelayedSend": 0,
            }),
        ),
        (
            JMAP_CONTACTS_CAPABILITY.to_string(),
            json!({
                "maxAddressBooksPerCard": 1,
            }),
        ),
        (
            JMAP_CALENDARS_CAPABILITY.to_string(),
            json!({
                "maxCalendarsPerEvent": 1,
            }),
        ),
        (
            JMAP_TASKS_CAPABILITY.to_string(),
            json!({
                "minDateTime": "1970-01-01T00:00:00",
                "maxDateTime": "9999-12-31T23:59:59",
                "mayCreateTaskList": false,
            }),
        ),
        (
            JMAP_WEBSOCKET_CAPABILITY.to_string(),
            json!({
                "url": websocket_url,
                "supportsPush": true,
            }),
        ),
    ])
}

fn requested_account_id(
    requested_account_id: Option<&str>,
    account: &AuthenticatedAccount,
) -> Result<Uuid> {
    match requested_account_id {
        Some(value) => {
            let id = parse_uuid(value)?;
            if id == account.account_id {
                Ok(id)
            } else {
                bail!("accountId does not match authenticated account");
            }
        }
        None => Ok(account.account_id),
    }
}

fn parse_uuid(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| anyhow!("invalid id: {value}"))
}

fn parse_uuid_list(value: Option<Vec<String>>) -> Result<Option<Vec<Uuid>>> {
    value
        .map(|values| values.into_iter().map(|value| parse_uuid(&value)).collect())
        .transpose()
}

fn validate_query_sort(sort: Option<&[EmailQuerySort]>) -> Result<()> {
    if let Some(sort) = sort {
        for item in sort {
            if item.property != "receivedAt" || item.is_ascending.unwrap_or(false) {
                bail!("only receivedAt descending sort is supported");
            }
        }
    }
    Ok(())
}

fn validate_entity_sort(
    sort: Option<&[EntityQuerySort]>,
    expected_property: &str,
    ascending: bool,
) -> Result<()> {
    if let Some(sort) = sort {
        for item in sort {
            if item.property != expected_property || item.is_ascending.unwrap_or(true) != ascending
            {
                let direction = if ascending { "ascending" } else { "descending" };
                bail!("only {expected_property} {direction} sort is supported");
            }
        }
    }
    Ok(())
}

fn validate_contact_filter(filter: Option<&ContactCardQueryFilter>) -> Result<()> {
    if let Some(filter) = filter {
        if let Some(address_book_id) = filter.in_address_book.as_deref() {
            require_collection_id(address_book_id, "addressBook")?;
        }
    }
    Ok(())
}

fn validate_calendar_event_filter(filter: Option<&CalendarEventQueryFilter>) -> Result<()> {
    if let Some(filter) = filter {
        if let Some(calendar_id) = filter.in_calendar.as_deref() {
            require_collection_id(calendar_id, "calendar")?;
        }
        if let Some(after) = filter.after.as_deref() {
            parse_local_datetime(after)?;
        }
        if let Some(before) = filter.before.as_deref() {
            parse_local_datetime(before)?;
        }
    }
    Ok(())
}

fn validate_task_sort(sort: Option<&[TaskQuerySort]>) -> Result<()> {
    if let Some(sort) = sort {
        for item in sort {
            if item.property != "sortOrder" || item.is_ascending.unwrap_or(true) != true {
                bail!("only sortOrder ascending sort is supported");
            }
        }
    }
    Ok(())
}

fn validate_task_filter(filter: Option<&TaskQueryFilter>) -> Result<()> {
    if let Some(filter) = filter {
        if let Some(task_list_id) = filter.in_task_list.as_deref() {
            if task_list_id != default_task_list_id() {
                bail!("only taskListId=default is supported");
            }
        }
        if let Some(status) = filter.status.as_deref() {
            validate_task_status_value(status)?;
        }
    }
    Ok(())
}

fn require_collection_id(value: &str, kind: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{kind} id is required");
    }
    Ok(())
}

fn default_task_list_id() -> &'static str {
    "default"
}

fn validate_task_status_value(status: &str) -> Result<()> {
    match status.trim().to_ascii_lowercase().as_str() {
        "" | "needs-action" | "in-progress" | "completed" | "cancelled" => Ok(()),
        other => bail!("unsupported task status: {other}"),
    }
}

fn mailbox_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "role".to_string(),
                "sortOrder".to_string(),
                "totalEmails".to_string(),
                "unreadEmails".to_string(),
                "isSubscribed".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn address_book_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "sortOrder".to_string(),
                "isSubscribed".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn address_book_to_value(
    collection: &CollaborationCollection,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", collection.id.clone());
    insert_if(
        properties,
        &mut object,
        "name",
        collection.display_name.clone(),
    );
    insert_if(properties, &mut object, "sortOrder", 0);
    insert_if(properties, &mut object, "isSubscribed", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayRead": collection.rights.may_read,
                "mayAddItems": collection.rights.may_write,
                "mayModifyItems": collection.rights.may_write,
                "mayRemoveItems": collection.rights.may_delete,
                "mayRename": false,
                "mayDelete": false,
                "mayAdmin": collection.rights.may_share,
            }),
        );
    }
    Value::Object(object)
}

fn calendar_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "sortOrder".to_string(),
                "isSubscribed".to_string(),
                "isVisible".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn task_list_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "role".to_string(),
                "sortOrder".to_string(),
                "isSubscribed".to_string(),
                "isVisible".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn calendar_to_value(collection: &CollaborationCollection, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", collection.id.clone());
    insert_if(
        properties,
        &mut object,
        "name",
        collection.display_name.clone(),
    );
    insert_if(properties, &mut object, "sortOrder", 0);
    insert_if(properties, &mut object, "isSubscribed", true);
    insert_if(properties, &mut object, "isVisible", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayRead": collection.rights.may_read,
                "mayAddItems": collection.rights.may_write,
                "mayModifyItems": collection.rights.may_write,
                "mayRemoveItems": collection.rights.may_delete,
                "mayRename": false,
                "mayDelete": false,
                "mayAdmin": collection.rights.may_share,
            }),
        );
    }
    Value::Object(object)
}

fn task_list_to_value(properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", default_task_list_id());
    insert_if(properties, &mut object, "name", "Tasks");
    insert_if(properties, &mut object, "role", "inbox");
    insert_if(properties, &mut object, "sortOrder", 0);
    insert_if(properties, &mut object, "isSubscribed", true);
    insert_if(properties, &mut object, "isVisible", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayRead": true,
                "mayAddItems": true,
                "mayModifyItems": true,
                "mayRemoveItems": true,
                "mayRename": false,
                "mayDelete": false,
                "mayAdmin": false,
            }),
        );
    }
    Value::Object(object)
}

fn mailbox_to_value(
    mailbox: &JmapMailbox,
    access: &MailboxAccountAccess,
    properties: &HashSet<String>,
) -> Value {
    let is_drafts = mailbox.role == "drafts";
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", mailbox.id.to_string());
    insert_if(properties, &mut object, "name", mailbox.name.clone());
    insert_if(properties, &mut object, "role", mailbox.role.clone());
    insert_if(properties, &mut object, "sortOrder", mailbox.sort_order);
    insert_if(properties, &mut object, "totalEmails", mailbox.total_emails);
    insert_if(
        properties,
        &mut object,
        "unreadEmails",
        mailbox.unread_emails,
    );
    insert_if(properties, &mut object, "isSubscribed", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayReadItems": access.may_read,
                "mayAddItems": access.may_write && is_drafts,
                "mayRemoveItems": access.may_write && is_drafts,
                "maySetSeen": access.may_write,
                "maySetKeywords": access.may_write,
                "mayCreateChild": false,
                "mayRename": false,
                "mayDelete": false,
                "maySubmit": is_drafts && mailbox_account_may_submit(access),
            }),
        );
    }
    Value::Object(object)
}

fn collection_state_fingerprint(collection: &CollaborationCollection) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        collection.kind,
        collection.owner_account_id,
        collection.owner_email,
        collection.owner_display_name,
        collection.display_name,
        collection.is_owned,
        collection.rights.may_read,
        collection.rights.may_write,
        collection.rights.may_delete,
        collection.rights.may_share
    )
}

fn contact_state_fingerprint(contact: &AccessibleContact) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        contact.collection_id,
        contact.owner_account_id,
        contact.owner_email,
        contact.owner_display_name,
        contact.name,
        contact.role,
        contact.email,
        contact.phone,
        contact.team,
        contact.notes,
        contact.rights.may_write,
        contact.rights.may_delete
    )
}

fn event_state_fingerprint(event: &AccessibleEvent) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        event.collection_id,
        event.owner_account_id,
        event.owner_email,
        event.owner_display_name,
        event.date,
        event.time,
        event.time_zone,
        event.duration_minutes,
        event.recurrence_rule,
        event.title,
        event.location,
        event.attendees,
        event.attendees_json,
        event.notes,
        event.rights.may_write
    )
}

fn task_state_fingerprint(task: &ClientTask) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        task.title,
        task.description,
        task.status,
        task.due_at.as_deref().unwrap_or_default(),
        task.completed_at.as_deref().unwrap_or_default(),
        task.sort_order,
        task.updated_at
    )
}

fn default_task_list_state_fingerprint() -> String {
    "default|Tasks|inbox|0|rw".to_string()
}

fn email_state_fingerprint(email: &JmapEmail) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        email.thread_id,
        email.mailbox_id,
        email.mailbox_role,
        email.mailbox_name,
        email.received_at,
        email.sent_at.as_deref().unwrap_or_default(),
        email.from_display.as_deref().unwrap_or_default(),
        email.from_address,
        format_addresses(&email.to),
        format_addresses(&email.cc),
        format_addresses(&email.bcc),
        email.subject,
        email.preview,
        email.unread,
        email.flagged,
        email.delivery_status,
    ) + &format!(
        "|{}|{}|{}|{}|{}",
        email.body_text,
        email.body_html_sanitized.as_deref().unwrap_or_default(),
        email.has_attachments,
        email.size_octets,
        email.internet_message_id.as_deref().unwrap_or_default(),
    )
}

fn format_addresses(addresses: &[JmapEmailAddress]) -> String {
    addresses
        .iter()
        .map(|address| {
            format!(
                "{}:{}",
                address.address,
                address.display_name.clone().unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn changes_response(
    account_id: Uuid,
    kind: &str,
    since_state: &str,
    max_changes: Option<u64>,
    current_entries: Vec<StateEntry>,
) -> Value {
    let max_changes = max_changes.unwrap_or(u64::MAX) as usize;
    let new_state =
        encode_state(kind, current_entries.clone()).unwrap_or_else(|_| SESSION_STATE.to_string());
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

fn encode_state(kind: &str, entries: Vec<StateEntry>) -> Result<String> {
    let mut entries = entries;
    entries.sort_by(|left, right| left.id.cmp(&right.id));
    let token = StateToken {
        version: STATE_TOKEN_VERSION.to_string(),
        kind: kind.to_string(),
        entries,
    };
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token)?))
}

fn decode_state(value: &str) -> Result<StateToken> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| anyhow!("invalid state"))?;
    let token: StateToken = serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid state"))?;
    if token.version != STATE_TOKEN_VERSION {
        bail!("unsupported state version");
    }
    Ok(token)
}

fn encode_push_state(type_states: &HashMap<String, HashMap<String, String>>) -> Result<String> {
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

fn encode_query_state(
    kind: &str,
    filter: Option<Value>,
    sort: Option<Vec<Value>>,
    ids: Vec<String>,
) -> Result<String> {
    let token = QueryStateToken {
        version: QUERY_STATE_VERSION.to_string(),
        kind: kind.to_string(),
        filter,
        sort,
        ids,
    };
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token)?))
}

fn decode_query_state(value: &str) -> Result<QueryStateToken> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| anyhow!("invalid queryState"))?;
    let token: QueryStateToken =
        serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid queryState"))?;
    if token.version != QUERY_STATE_VERSION {
        bail!("unsupported queryState version");
    }
    Ok(token)
}

fn query_changes_response(
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

fn compute_query_diff(
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

fn compute_query_diff_with_reorders(
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

fn full_query_limit(total: u64) -> u64 {
    total.max(1).min(i64::MAX as u64)
}

fn parse_upload_blob_id(value: &str) -> Result<Uuid> {
    match JmapBlobId::parse(value)? {
        JmapBlobId::Upload(id) => Ok(id),
        JmapBlobId::Opaque(_) => bail!("blob not found"),
    }
}

fn expected_attachment_kind(media_type: &str, file_name: &str) -> ExpectedKind {
    let media_type = media_type.trim().to_ascii_lowercase();
    let file_name = file_name.trim().to_ascii_lowercase();
    if matches!(
        media_type.as_str(),
        "application/pdf"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.oasis.opendocument.text"
    ) || file_name.ends_with(".pdf")
        || file_name.ends_with(".docx")
        || file_name.ends_with(".odt")
    {
        ExpectedKind::SupportedAttachmentText
    } else {
        ExpectedKind::Any
    }
}

fn serialize_email_query_filter(filter: &EmailQueryFilter) -> Result<Value> {
    Ok(serde_json::to_value(filter)?)
}

fn serialize_email_query_sort(sort: &[EmailQuerySort]) -> Result<Vec<Value>> {
    sort.iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn blob_id_for_message(email: &JmapEmail) -> String {
    JmapBlobId::for_message(email).into_response_id()
}

impl JmapBlobId {
    fn parse(value: &str) -> Result<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            bail!("blobId is required");
        }
        if let Ok(id) = Uuid::parse_str(trimmed) {
            return Ok(Self::Upload(id));
        }
        if let Some(upload_id) = trimmed.strip_prefix("upload:") {
            return Ok(Self::Upload(parse_uuid(upload_id)?));
        }
        Ok(Self::Opaque(trimmed.to_string()))
    }

    fn for_message(email: &JmapEmail) -> Self {
        match email.mime_blob_ref.as_deref() {
            Some(value) if !value.trim().is_empty() => {
                Self::parse(value).unwrap_or_else(|_| Self::Opaque(value.trim().to_string()))
            }
            _ => Self::Opaque(format!("message:{}", email.id)),
        }
    }

    fn into_response_id(self) -> String {
        match self {
            Self::Upload(id) => format!("upload:{id}"),
            Self::Opaque(value) => value,
        }
    }
}

fn email_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "blobId".to_string(),
                "threadId".to_string(),
                "mailboxIds".to_string(),
                "keywords".to_string(),
                "size".to_string(),
                "receivedAt".to_string(),
                "sentAt".to_string(),
                "messageId".to_string(),
                "subject".to_string(),
                "from".to_string(),
                "sender".to_string(),
                "to".to_string(),
                "cc".to_string(),
                "preview".to_string(),
                "hasAttachment".to_string(),
                "textBody".to_string(),
                "htmlBody".to_string(),
                "bodyValues".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn contact_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "uid".to_string(),
                "kind".to_string(),
                "name".to_string(),
                "emails".to_string(),
                "phones".to_string(),
                "organizations".to_string(),
                "titles".to_string(),
                "notes".to_string(),
                "addressBookIds".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn calendar_event_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "uid".to_string(),
                "@type".to_string(),
                "title".to_string(),
                "start".to_string(),
                "duration".to_string(),
                "timeZone".to_string(),
                "locations".to_string(),
                "participants".to_string(),
                "description".to_string(),
                "calendarIds".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn task_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "uid".to_string(),
                "@type".to_string(),
                "taskListId".to_string(),
                "title".to_string(),
                "description".to_string(),
                "status".to_string(),
                "due".to_string(),
                "completed".to_string(),
                "sortOrder".to_string(),
                "updated".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn email_submission_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "emailId".to_string(),
                "threadId".to_string(),
                "identityId".to_string(),
                "envelope".to_string(),
                "sendAt".to_string(),
                "undoStatus".to_string(),
                "deliveryStatus".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn identity_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
                "replyTo".to_string(),
                "bcc".to_string(),
                "textSignature".to_string(),
                "htmlSignature".to_string(),
                "mayDelete".to_string(),
                "xLpeOwnerAccountId".to_string(),
                "xLpeAuthorizationKind".to_string(),
                "xLpeSender".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn thread_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| vec!["id".to_string(), "emailIds".to_string()])
        .into_iter()
        .collect()
}

fn email_to_value(email: &JmapEmail, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", email.id.to_string());
    insert_if(
        properties,
        &mut object,
        "blobId",
        blob_id_for_message(email),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        email.thread_id.to_string(),
    );
    if properties.contains("mailboxIds") {
        let mut mailbox_ids = Map::new();
        mailbox_ids.insert(email.mailbox_id.to_string(), Value::Bool(true));
        object.insert("mailboxIds".to_string(), Value::Object(mailbox_ids));
    }
    if properties.contains("keywords") {
        object.insert("keywords".to_string(), email_keywords(email));
    }
    insert_if(properties, &mut object, "size", email.size_octets);
    insert_if(
        properties,
        &mut object,
        "receivedAt",
        email.received_at.clone(),
    );
    if let Some(sent_at) = &email.sent_at {
        insert_if(properties, &mut object, "sentAt", sent_at.clone());
    }
    if properties.contains("messageId") {
        object.insert(
            "messageId".to_string(),
            Value::Array(
                email
                    .internet_message_id
                    .as_ref()
                    .map(|message_id| vec![Value::String(message_id.clone())])
                    .unwrap_or_default(),
            ),
        );
    }
    insert_if(properties, &mut object, "subject", email.subject.clone());
    if properties.contains("from") {
        object.insert(
            "from".to_string(),
            Value::Array(vec![address_value(
                &email.from_address,
                email.from_display.as_deref(),
            )]),
        );
    }
    if properties.contains("sender") && email.sender_address.is_some() {
        object.insert(
            "sender".to_string(),
            Value::Array(vec![address_value(
                email.sender_address.as_deref().unwrap_or_default(),
                email.sender_display.as_deref(),
            )]),
        );
    }
    if properties.contains("to") {
        object.insert(
            "to".to_string(),
            Value::Array(
                email
                    .to
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    if properties.contains("cc") {
        object.insert(
            "cc".to_string(),
            Value::Array(
                email
                    .cc
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    if properties.contains("bcc") && !email.bcc.is_empty() {
        object.insert(
            "bcc".to_string(),
            Value::Array(
                email
                    .bcc
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    insert_if(properties, &mut object, "preview", email.preview.clone());
    insert_if(
        properties,
        &mut object,
        "hasAttachment",
        email.has_attachments,
    );

    let mut body_values = Map::new();
    if !email.body_text.is_empty() {
        body_values.insert(
            "textBody".to_string(),
            json!({
                "value": email.body_text.clone(),
                "isEncodingProblem": false,
                "isTruncated": false,
            }),
        );
        if properties.contains("textBody") {
            object.insert(
                "textBody".to_string(),
                json!([{ "partId": "textBody", "type": "text/plain" }]),
            );
        }
    }
    if let Some(html) = &email.body_html_sanitized {
        body_values.insert(
            "htmlBody".to_string(),
            json!({
                "value": html.clone(),
                "isEncodingProblem": false,
                "isTruncated": false,
            }),
        );
        if properties.contains("htmlBody") {
            object.insert(
                "htmlBody".to_string(),
                json!([{ "partId": "htmlBody", "type": "text/html" }]),
            );
        }
    }
    if properties.contains("bodyValues") {
        object.insert("bodyValues".to_string(), Value::Object(body_values));
    }

    Value::Object(object)
}

fn email_submission_to_value(
    submission: &JmapEmailSubmission,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", submission.id.to_string());
    insert_if(
        properties,
        &mut object,
        "emailId",
        submission.email_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        submission.thread_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "identityId",
        submission.identity_id.clone(),
    );
    if properties.contains("envelope") {
        object.insert(
            "envelope".to_string(),
            json!({
                "mailFrom": {"email": submission.envelope_mail_from},
                "rcptTo": submission.envelope_rcpt_to.iter().map(|address| json!({"email": address})).collect::<Vec<_>>(),
            }),
        );
    }
    insert_if(
        properties,
        &mut object,
        "sendAt",
        submission.send_at.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "undoStatus",
        submission.undo_status.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "deliveryStatus",
        submission.delivery_status.clone(),
    );
    Value::Object(object)
}

fn identity_to_value(identity: &SenderIdentity, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", identity.id.clone());
    insert_if(
        properties,
        &mut object,
        "name",
        identity.display_name.clone(),
    );
    insert_if(properties, &mut object, "email", identity.email.clone());
    if properties.contains("replyTo") {
        object.insert("replyTo".to_string(), Value::Null);
    }
    if properties.contains("bcc") {
        object.insert("bcc".to_string(), Value::Null);
    }
    insert_if(properties, &mut object, "textSignature", "");
    insert_if(properties, &mut object, "htmlSignature", "");
    insert_if(properties, &mut object, "mayDelete", false);
    insert_if(
        properties,
        &mut object,
        "xLpeOwnerAccountId",
        identity.owner_account_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "xLpeAuthorizationKind",
        identity.authorization_kind.clone(),
    );
    if properties.contains("xLpeSender") {
        let sender = identity.sender_address.as_ref().map(|address| {
            json!({
                "email": address,
                "name": identity.sender_display.clone(),
            })
        });
        object.insert("xLpeSender".to_string(), sender.unwrap_or(Value::Null));
    }
    Value::Object(object)
}

fn mailbox_account_may_submit(access: &MailboxAccountAccess) -> bool {
    access.is_owned || access.may_send_as || access.may_send_on_behalf
}

fn mailbox_account_is_read_only(access: &MailboxAccountAccess) -> bool {
    !(access.may_write || mailbox_account_may_submit(access))
}

fn thread_to_value(thread_id: Uuid, email_ids: Vec<String>, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", thread_id.to_string());
    if properties.contains("emailIds") {
        object.insert(
            "emailIds".to_string(),
            Value::Array(email_ids.into_iter().map(Value::String).collect()),
        );
    }
    Value::Object(object)
}

fn search_snippet_to_value(email: &JmapEmail) -> Value {
    let subject = if email.subject.is_empty() {
        email.preview.clone()
    } else {
        email.subject.clone()
    };
    let preview = if email.preview.is_empty() {
        trim_snippet(&email.body_text, 120)
    } else {
        trim_snippet(&email.preview, 120)
    };
    json!({
        "emailId": email.id.to_string(),
        "subject": subject,
        "preview": preview,
    })
}

fn trim_snippet(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        normalized.chars().take(max_chars).collect::<String>()
    }
}

fn quota_to_value(quota: &JmapQuota) -> Value {
    json!({
        "id": quota.id,
        "name": quota.name,
        "used": quota.used,
        "hardLimit": quota.hard_limit,
        "scope": "account",
    })
}

fn email_keywords(email: &JmapEmail) -> Value {
    let mut keywords = Map::new();
    if email.mailbox_role == "drafts" {
        keywords.insert("$draft".to_string(), Value::Bool(true));
    }
    if !email.unread {
        keywords.insert("$seen".to_string(), Value::Bool(true));
    }
    if email.flagged {
        keywords.insert("$flagged".to_string(), Value::Bool(true));
    }
    Value::Object(keywords)
}

fn contact_to_value(contact: &AccessibleContact, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", contact.id.to_string());
    insert_if(properties, &mut object, "uid", contact.id.to_string());
    insert_if(properties, &mut object, "kind", "individual");
    if properties.contains("name") {
        object.insert(
            "name".to_string(),
            json!({
                "@type": "Name",
                "full": contact.name,
            }),
        );
    }
    if properties.contains("emails") && !contact.email.trim().is_empty() {
        object.insert(
            "emails".to_string(),
            json!({
                "main": {
                    "@type": "EmailAddress",
                    "address": contact.email,
                    "contexts": {"work": true},
                    "pref": 1,
                }
            }),
        );
    }
    if properties.contains("phones") && !contact.phone.trim().is_empty() {
        object.insert(
            "phones".to_string(),
            json!({
                "main": {
                    "@type": "Phone",
                    "number": contact.phone,
                    "contexts": {"work": true},
                }
            }),
        );
    }
    if properties.contains("organizations") && !contact.team.trim().is_empty() {
        object.insert(
            "organizations".to_string(),
            json!({
                "main": {
                    "@type": "Organization",
                    "name": contact.team,
                    "contexts": {"work": true},
                }
            }),
        );
    }
    if properties.contains("titles") && !contact.role.trim().is_empty() {
        object.insert(
            "titles".to_string(),
            json!({
                "main": {
                    "@type": "Title",
                    "kind": "title",
                    "name": contact.role,
                }
            }),
        );
    }
    if properties.contains("notes") && !contact.notes.trim().is_empty() {
        object.insert(
            "notes".to_string(),
            json!({
                "main": {
                    "@type": "Note",
                    "note": contact.notes,
                }
            }),
        );
    }
    if properties.contains("addressBookIds") {
        object.insert(
            "addressBookIds".to_string(),
            json!({contact.collection_id.clone(): true}),
        );
    }
    Value::Object(object)
}

fn calendar_event_to_value(event: &AccessibleEvent, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", event.id.to_string());
    insert_if(properties, &mut object, "uid", event.id.to_string());
    insert_if(properties, &mut object, "@type", "Event");
    insert_if(properties, &mut object, "title", event.title.clone());
    insert_if(
        properties,
        &mut object,
        "start",
        format!("{}T{}:00", event.date, event.time),
    );
    insert_if(
        properties,
        &mut object,
        "duration",
        if event.duration_minutes <= 0 {
            "PT0S".to_string()
        } else {
            format!("PT{}M", event.duration_minutes)
        },
    );
    if properties.contains("timeZone") {
        if event.time_zone.trim().is_empty() {
            object.insert("timeZone".to_string(), Value::Null);
        } else {
            object.insert(
                "timeZone".to_string(),
                Value::String(event.time_zone.clone()),
            );
        }
    }
    if properties.contains("locations") && !event.location.trim().is_empty() {
        object.insert(
            "locations".to_string(),
            json!({
                "main": {
                    "@type": "Location",
                    "name": event.location,
                }
            }),
        );
    }
    if properties.contains("participants") {
        let participants = participants_from_event(event);
        if participants
            .as_object()
            .map(|entries| !entries.is_empty())
            .unwrap_or(false)
        {
            object.insert("participants".to_string(), participants);
        }
    }
    insert_if(properties, &mut object, "description", event.notes.clone());
    if properties.contains("calendarIds") {
        object.insert(
            "calendarIds".to_string(),
            json!({event.collection_id.clone(): true}),
        );
    }
    Value::Object(object)
}

fn task_to_value(task: &ClientTask, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", task.id.to_string());
    insert_if(properties, &mut object, "uid", task.id.to_string());
    insert_if(properties, &mut object, "@type", "Task");
    insert_if(
        properties,
        &mut object,
        "taskListId",
        default_task_list_id(),
    );
    insert_if(properties, &mut object, "title", task.title.clone());
    insert_if(
        properties,
        &mut object,
        "description",
        task.description.clone(),
    );
    insert_if(properties, &mut object, "status", task.status.clone());
    insert_if(properties, &mut object, "due", task.due_at.clone());
    insert_if(
        properties,
        &mut object,
        "completed",
        task.completed_at.clone(),
    );
    insert_if(properties, &mut object, "sortOrder", task.sort_order);
    insert_if(properties, &mut object, "updated", task.updated_at.clone());
    Value::Object(object)
}

fn participants_from_event(event: &AccessibleEvent) -> Value {
    let metadata = parse_calendar_participants_metadata(&event.attendees_json);
    if metadata.organizer.is_some() || !metadata.attendees.is_empty() {
        let mut participants = Map::new();
        if let Some(organizer) = metadata.organizer {
            participants.insert(
                "owner".to_string(),
                participant_value(
                    &organizer.common_name,
                    &organizer.email,
                    json!({"owner": true}),
                    None,
                    false,
                ),
            );
        }
        for (index, attendee) in metadata.attendees.iter().enumerate() {
            let mut roles = Map::new();
            roles.insert("attendee".to_string(), Value::Bool(true));
            if attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT") {
                roles.insert("optional".to_string(), Value::Bool(true));
            }
            participants.insert(
                format!("p{}", index + 1),
                participant_value(
                    &attendee.common_name,
                    &attendee.email,
                    Value::Object(roles),
                    Some(&attendee.partstat),
                    attendee.rsvp,
                ),
            );
        }
        return Value::Object(participants);
    }
    participants_from_attendees(&event.attendees)
}

fn participants_from_attendees(attendees: &str) -> Value {
    let participants = attendees
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .enumerate()
        .map(|(index, value)| {
            let key = format!("p{}", index + 1);
            let participant = if value.contains('@') {
                json!({
                    "@type": "Participant",
                    "name": value,
                    "email": value,
                    "roles": {"attendee": true},
                })
            } else {
                json!({
                    "@type": "Participant",
                    "name": value,
                    "roles": {"attendee": true},
                })
            };
            (key, participant)
        })
        .collect::<Map<String, Value>>();
    Value::Object(participants)
}

fn participant_value(
    name: &str,
    email: &str,
    roles: Value,
    participation_status: Option<&str>,
    expect_reply: bool,
) -> Value {
    let mut participant = Map::new();
    participant.insert(
        "@type".to_string(),
        Value::String("Participant".to_string()),
    );
    if !name.trim().is_empty() {
        participant.insert("name".to_string(), Value::String(name.trim().to_string()));
    }
    if !email.trim().is_empty() {
        participant.insert("email".to_string(), Value::String(email.trim().to_string()));
        participant.insert(
            "sendTo".to_string(),
            json!({"imip": format!("mailto:{}", email.trim())}),
        );
    }
    participant.insert("roles".to_string(), roles);
    if let Some(status) = participation_status {
        participant.insert(
            "participationStatus".to_string(),
            Value::String(normalize_calendar_participation_status(status)),
        );
    }
    if expect_reply {
        participant.insert("expectReply".to_string(), Value::Bool(true));
    }
    Value::Object(participant)
}

fn contact_matches_filter(contact: &AccessibleContact, filter: &ContactCardQueryFilter) -> bool {
    if let Some(address_book_id) = filter.in_address_book.as_deref() {
        if address_book_id != contact.collection_id {
            return false;
        }
    }
    if let Some(text) = filter.text.as_deref() {
        let needle = text.trim().to_lowercase();
        if !needle.is_empty() {
            let haystack = format!(
                "{} {} {} {} {} {}",
                contact.name,
                contact.email,
                contact.role,
                contact.phone,
                contact.team,
                contact.notes
            )
            .to_lowercase();
            if !haystack.contains(&needle) {
                return false;
            }
        }
    }
    true
}

fn event_matches_filter(event: &AccessibleEvent, filter: &CalendarEventQueryFilter) -> bool {
    if let Some(calendar_id) = filter.in_calendar.as_deref() {
        if calendar_id != event.collection_id {
            return false;
        }
    }
    if let Some(after) = filter.after.as_deref() {
        let start = calendar_event_start(event);
        if let Ok(after) = parse_local_datetime_value(after) {
            if start < after {
                return false;
            }
        } else {
            return false;
        }
    }
    if let Some(before) = filter.before.as_deref() {
        let start = calendar_event_start(event);
        if let Ok(before) = parse_local_datetime_value(before) {
            if start >= before {
                return false;
            }
        } else {
            return false;
        }
    }
    if let Some(text) = filter.text.as_deref() {
        let needle = text.trim().to_lowercase();
        if !needle.is_empty() {
            let haystack = format!(
                "{} {} {} {}",
                event.title, event.location, event.attendees, event.notes
            )
            .to_lowercase();
            if !haystack.contains(&needle) {
                return false;
            }
        }
    }
    true
}

fn task_matches_filter(task: &ClientTask, filter: &TaskQueryFilter) -> bool {
    if let Some(task_list_id) = filter.in_task_list.as_deref() {
        if task_list_id != default_task_list_id() {
            return false;
        }
    }
    if let Some(status) = filter.status.as_deref() {
        if task.status != status.trim().to_ascii_lowercase() {
            return false;
        }
    }
    if let Some(text) = filter.text.as_deref() {
        let text = text.trim().to_ascii_lowercase();
        if !text.is_empty()
            && !task.title.to_ascii_lowercase().contains(&text)
            && !task.description.to_ascii_lowercase().contains(&text)
        {
            return false;
        }
    }
    true
}

fn calendar_event_sort_key(event: &AccessibleEvent) -> String {
    calendar_event_start(event)
}

fn task_sort_key(task: &ClientTask) -> (i32, String, String) {
    (
        task.sort_order,
        task.updated_at.clone(),
        task.id.to_string(),
    )
}

fn calendar_event_start(event: &AccessibleEvent) -> String {
    format!("{}T{}:00", event.date, event.time)
}

fn insert_if<T: Serialize>(
    properties: &HashSet<String>,
    object: &mut Map<String, Value>,
    key: &str,
    value: T,
) {
    if properties.contains(key) {
        object.insert(
            key.to_string(),
            serde_json::to_value(value).unwrap_or(Value::Null),
        );
    }
}

fn address_value(email: &str, name: Option<&str>) -> Value {
    json!({
        "email": email,
        "name": name,
    })
}

fn method_error(kind: &str, description: &str) -> Value {
    json!({
        "type": kind,
        "description": description,
    })
}

fn set_error(description: &str) -> Value {
    method_error("invalidProperties", description)
}

fn parse_submission_email_id(
    value: &Value,
    created_ids: &HashMap<String, String>,
) -> Result<Option<String>> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("submission create arguments must be an object"))?;
    if let Some(email_id) = object.get("emailId").and_then(Value::as_str) {
        return Ok(Some(resolve_creation_reference(email_id, created_ids)));
    }
    if let Some(reference) = object.get("#emailId").and_then(Value::as_str) {
        return Ok(created_ids.get(reference).cloned());
    }
    Ok(None)
}

fn resolve_creation_reference(value: &str, created_ids: &HashMap<String, String>) -> String {
    if let Some(reference) = value.strip_prefix('#') {
        created_ids
            .get(reference)
            .cloned()
            .unwrap_or_else(|| value.to_string())
    } else {
        value.to_string()
    }
}

fn parse_draft_mutation(value: Value) -> Result<DraftMutation> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("email arguments must be an object"))?;
    reject_unknown_email_properties(object)?;

    if let Some(mailbox_ids) = object.get("mailboxIds").and_then(Value::as_object) {
        if mailbox_ids.len() > 1 {
            bail!("only one mailboxId is supported");
        }
    }
    let keywords = parse_draft_keywords(object.get("keywords"))?;

    Ok(DraftMutation {
        from: parse_address_list(object.get("from"))?,
        sender: parse_address_list(object.get("sender"))?,
        to: parse_address_list(object.get("to"))?,
        cc: parse_address_list(object.get("cc"))?,
        bcc: parse_address_list(object.get("bcc"))?,
        subject: parse_optional_string(object.get("subject"))?,
        text_body: parse_optional_string(object.get("textBody"))?,
        html_body: parse_optional_nullable_string(object.get("htmlBody"))?,
        unread: keywords.unread,
        flagged: keywords.flagged,
    })
}

#[derive(Default)]
struct ParsedDraftKeywords {
    unread: Option<bool>,
    flagged: Option<bool>,
}

fn parse_draft_keywords(value: Option<&Value>) -> Result<ParsedDraftKeywords> {
    let Some(keywords) = value.and_then(Value::as_object) else {
        return Ok(ParsedDraftKeywords::default());
    };

    let mut parsed = ParsedDraftKeywords::default();
    for (keyword, enabled) in keywords {
        let enabled = enabled
            .as_bool()
            .ok_or_else(|| anyhow!("keyword {keyword} must be a boolean"))?;
        match keyword.as_str() {
            "$draft" => {
                if !enabled {
                    bail!("Email/set is limited to draft messages");
                }
            }
            "$seen" => parsed.unread = Some(!enabled),
            "$flagged" => parsed.flagged = Some(enabled),
            _ => bail!("unsupported keyword: {keyword}"),
        }
    }

    Ok(parsed)
}

fn parse_mailbox_create(value: Value) -> Result<MailboxCreateInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("mailbox create arguments must be an object"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("mailbox name is required"))?
        .to_string();
    let sort_order = object
        .get("sortOrder")
        .and_then(Value::as_i64)
        .map(|value| value as i32);
    Ok(MailboxCreateInput { name, sort_order })
}

fn parse_mailbox_update(value: Value) -> Result<MailboxUpdateInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("mailbox update arguments must be an object"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_string());
    let sort_order = object
        .get("sortOrder")
        .and_then(Value::as_i64)
        .map(|value| value as i32);
    Ok(MailboxUpdateInput { name, sort_order })
}

fn parse_contact_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<(Option<String>, UpsertClientContactInput)> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("contact card arguments must be an object"))?;
    reject_unknown_contact_properties(object)?;
    let collection_id = validate_address_book_ids(object.get("addressBookIds"))?;

    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("individual");
    if kind != "individual" {
        bail!("only kind=individual is supported");
    }

    Ok((
        collection_id,
        UpsertClientContactInput {
            id,
            account_id,
            name: parse_contact_name(object.get("name"))?,
            role: parse_contact_title(object.get("titles"))?,
            email: parse_contact_email(object.get("emails"))?,
            phone: parse_contact_phone(object.get("phones"))?,
            team: parse_contact_organization(object.get("organizations"))?,
            notes: parse_contact_note(object.get("notes"))?,
        },
    ))
}

fn parse_calendar_event_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<(Option<String>, UpsertClientEventInput)> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("calendar event arguments must be an object"))?;
    reject_unknown_calendar_event_properties(object)?;
    let collection_id = validate_calendar_ids(object.get("calendarIds"))?;

    let event_type = object
        .get("@type")
        .and_then(Value::as_str)
        .unwrap_or("Event");
    if event_type != "Event" {
        bail!("only @type=Event is supported");
    }
    if let Some(uid) = object.get("uid").and_then(Value::as_str) {
        if uid.trim().is_empty() {
            bail!("uid must not be empty");
        }
    }

    let (date, time) = parse_local_datetime(
        object
            .get("start")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("start is required"))?,
    )?;

    Ok((
        collection_id,
        UpsertClientEventInput {
            id,
            account_id,
            date,
            time,
            time_zone: parse_optional_string(object.get("timeZone"))?.unwrap_or_default(),
            duration_minutes: parse_calendar_duration(object.get("duration"))?,
            recurrence_rule: String::new(),
            title: parse_required_string(object.get("title"), "title")?,
            location: parse_calendar_location(object.get("locations"))?,
            attendees: parse_calendar_participants(object.get("participants"))?,
            attendees_json: parse_calendar_participants_json(object.get("participants"))?,
            notes: parse_optional_string(object.get("description"))?.unwrap_or_default(),
        },
    ))
}

fn parse_task_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<UpsertClientTaskInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("task arguments must be an object"))?;
    reject_unknown_task_properties(object)?;
    validate_task_list_id(object.get("taskListId"))?;

    let task_type = object
        .get("@type")
        .and_then(Value::as_str)
        .unwrap_or("Task");
    if task_type != "Task" {
        bail!("only @type=Task is supported");
    }
    if let Some(uid) = object.get("uid").and_then(Value::as_str) {
        if uid.trim().is_empty() {
            bail!("uid must not be empty");
        }
    }

    Ok(UpsertClientTaskInput {
        id,
        account_id,
        title: parse_required_string(object.get("title"), "title")?,
        description: parse_optional_string(object.get("description"))?.unwrap_or_default(),
        status: parse_optional_string(object.get("status"))?
            .unwrap_or_else(|| "needs-action".to_string()),
        due_at: parse_optional_string(object.get("due"))?,
        completed_at: parse_optional_string(object.get("completed"))?,
        sort_order: object.get("sortOrder").and_then(Value::as_i64).unwrap_or(0) as i32,
    })
}

fn parse_email_copy(value: Value, created_ids: &HashMap<String, String>) -> Result<(Uuid, Uuid)> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("Email/copy create arguments must be an object"))?;
    let email_id = object
        .get("emailId")
        .and_then(Value::as_str)
        .map(|value| resolve_creation_reference(value, created_ids))
        .ok_or_else(|| anyhow!("emailId is required"))?;
    let mailbox_ids = object
        .get("mailboxIds")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("mailboxIds is required"))?;
    let mailbox_id = mailbox_ids
        .iter()
        .find(|(_, value)| value.as_bool().unwrap_or(false))
        .map(|(id, _)| parse_uuid(id))
        .transpose()?
        .ok_or_else(|| anyhow!("one target mailboxId is required"))?;
    Ok((parse_uuid(&email_id)?, mailbox_id))
}

fn reject_unknown_contact_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "uid" | "kind" | "name" | "emails" | "phones" | "organizations" | "titles"
            | "notes" | "addressBookIds" => {}
            _ => bail!("unsupported contact card property: {key}"),
        }
    }
    Ok(())
}

fn reject_unknown_calendar_event_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "@type" | "uid" | "title" | "start" | "duration" | "timeZone" | "locations"
            | "participants" | "description" | "calendarIds" => {}
            _ => bail!("unsupported calendar event property: {key}"),
        }
    }
    Ok(())
}

fn reject_unknown_task_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "@type" | "uid" | "title" | "description" | "status" | "due" | "completed"
            | "sortOrder" | "taskListId" => {}
            _ => bail!("unsupported task property: {key}"),
        }
    }
    Ok(())
}

fn validate_address_book_ids(value: Option<&Value>) -> Result<Option<String>> {
    if let Some(value) = value {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("addressBookIds must be an object"))?;
        if object.len() != 1 {
            bail!("exactly one addressBookId must be provided");
        }
        let (collection_id, enabled) = object.iter().next().unwrap();
        if enabled.as_bool() != Some(true) {
            bail!("addressBookIds entries must be true");
        }
        return Ok(Some(collection_id.clone()));
    }
    Ok(None)
}

fn validate_calendar_ids(value: Option<&Value>) -> Result<Option<String>> {
    if let Some(value) = value {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("calendarIds must be an object"))?;
        if object.len() != 1 {
            bail!("exactly one calendarId must be provided");
        }
        let (collection_id, enabled) = object.iter().next().unwrap();
        if enabled.as_bool() != Some(true) {
            bail!("calendarIds entries must be true");
        }
        return Ok(Some(collection_id.clone()));
    }
    Ok(None)
}

fn validate_task_list_id(value: Option<&Value>) -> Result<()> {
    if let Some(value) = value {
        let task_list_id = value
            .as_str()
            .ok_or_else(|| anyhow!("taskListId must be a string"))?;
        if task_list_id != default_task_list_id() {
            bail!("only taskListId=default is supported");
        }
    }
    Ok(())
}

fn reject_unknown_email_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "from" | "sender" | "to" | "cc" | "bcc" | "subject" | "textBody" | "htmlBody"
            | "mailboxIds" | "keywords" => {}
            _ => bail!("unsupported email property: {key}"),
        }
    }
    Ok(())
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}

fn parse_contact_name(value: Option<&Value>) -> Result<String> {
    let value = value.ok_or_else(|| anyhow!("name is required"))?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("name must be an object"))?;
    if let Some(full) = object.get("full").and_then(Value::as_str) {
        let full = full.trim();
        if full.is_empty() {
            bail!("name.full is required");
        }
        return Ok(full.to_string());
    }
    bail!("name.full is required")
}

fn parse_contact_email(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "emails", "address")
        .map(|email| normalize_email(&email))
}

fn parse_contact_phone(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "phones", "number")
}

fn parse_contact_organization(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "organizations", "name")
}

fn parse_contact_title(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "titles", "name")
}

fn parse_contact_note(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "notes", "note")
}

fn parse_calendar_location(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "locations", "name")
}

fn parse_calendar_participants(value: Option<&Value>) -> Result<String> {
    Ok(calendar_attendee_labels(&parse_jmap_calendar_participants(
        value,
    )?))
}

fn parse_calendar_participants_json(value: Option<&Value>) -> Result<String> {
    Ok(serialize_calendar_participants_metadata(
        &parse_jmap_calendar_participants(value)?,
    ))
}

fn parse_jmap_calendar_participants(value: Option<&Value>) -> Result<CalendarParticipantsMetadata> {
    let Some(value) = value else {
        return Ok(CalendarParticipantsMetadata::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("participants must be an object"))?;
    let mut metadata = CalendarParticipantsMetadata::default();
    for participant in object.values() {
        let participant = participant
            .as_object()
            .ok_or_else(|| anyhow!("participants entries must be objects"))?;
        let common_name = participant
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        let roles = participant.get("roles").and_then(Value::as_object);
        let is_owner = roles
            .and_then(|roles| roles.get("owner"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let email = participant_email(participant, is_owner)?;
        if email.is_empty() && common_name.is_empty() {
            bail!("participant name or email is required");
        }
        if is_owner {
            if metadata.organizer.is_some() {
                bail!("only one organizer participant is supported");
            }
            metadata.organizer = Some(CalendarOrganizerMetadata { email, common_name });
            continue;
        }
        metadata.attendees.push(CalendarParticipantMetadata {
            email,
            common_name,
            role: if roles
                .and_then(|roles| roles.get("optional"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "OPT-PARTICIPANT".to_string()
            } else {
                "REQ-PARTICIPANT".to_string()
            },
            partstat: normalize_calendar_participation_status(
                participant
                    .get("participationStatus")
                    .and_then(Value::as_str)
                    .unwrap_or("needs-action"),
            ),
            rsvp: participant
                .get("expectReply")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        });
    }
    Ok(metadata)
}

fn participant_email(participant: &Map<String, Value>, owner: bool) -> Result<String> {
    if let Some(email) = participant.get("email").and_then(Value::as_str) {
        let normalized = normalize_calendar_email(email);
        if !normalized.is_empty() {
            return Ok(normalized);
        }
    }
    if let Some(send_to) = participant.get("sendTo").and_then(Value::as_object) {
        if let Some(email) = send_to.get("imip").and_then(Value::as_str) {
            let normalized = normalize_calendar_email(email);
            if !normalized.is_empty() {
                return Ok(normalized);
            }
        }
    }
    if owner {
        bail!("organizer participant email is required");
    }
    Ok(String::new())
}

fn parse_calendar_duration(value: Option<&Value>) -> Result<i32> {
    let Some(value) = value.and_then(Value::as_str) else {
        return Ok(0);
    };
    if value == "PT0S" {
        return Ok(0);
    }
    let Some(value) = value.strip_prefix("PT") else {
        bail!("duration must use PT...");
    };
    if let Some(hours) = value.strip_suffix('H') {
        return hours
            .parse::<i32>()
            .map(|value| value.max(0) * 60)
            .map_err(|_| anyhow!("invalid duration"));
    }
    if let Some(minutes) = value.strip_suffix('M') {
        return minutes
            .parse::<i32>()
            .map(|value| value.max(0))
            .map_err(|_| anyhow!("invalid duration"));
    }
    bail!("invalid duration")
}

fn parse_first_property_object_string(
    value: Option<&Value>,
    property_name: &str,
    field_name: &str,
) -> Result<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("{property_name} must be an object"))?;
    let Some(first) = object.values().next() else {
        return Ok(String::new());
    };
    let first = first
        .as_object()
        .ok_or_else(|| anyhow!("{property_name} entries must be objects"))?;
    Ok(first
        .get(field_name)
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_string())
}

fn parse_address_list(value: Option<&Value>) -> Result<Option<Vec<EmailAddressInput>>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(Vec::new())),
        Some(value) => Ok(Some(serde_json::from_value(value.clone())?)),
    }
}

fn parse_optional_string(value: Option<&Value>) -> Result<Option<String>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(String::new())),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => bail!("string property expected"),
    }
}

fn parse_required_string(value: Option<&Value>, field_name: &str) -> Result<String> {
    let value = value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{field_name} is required"))?;
    Ok(value.to_string())
}

fn parse_optional_nullable_string(value: Option<&Value>) -> Result<Option<Option<String>>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(value)) => Ok(Some(Some(value.clone()))),
        _ => bail!("string or null property expected"),
    }
}

fn parse_local_datetime(value: &str) -> Result<(String, String)> {
    let trimmed = value.trim();
    let (date, time) = trimmed
        .split_once('T')
        .ok_or_else(|| anyhow!("invalid local date-time"))?;
    if date.len() != 10 || time.len() < 5 {
        bail!("invalid local date-time");
    }
    let time = time.trim_end_matches('Z');
    let hhmm = if let Some((hours_minutes, _seconds)) = time.split_once(':') {
        if hours_minutes.len() != 2 {
            bail!("invalid local date-time");
        }
        format!("{hours_minutes}:{}", &time[3..5])
    } else {
        bail!("invalid local date-time");
    };
    Ok((date.to_string(), hhmm))
}

fn parse_local_datetime_value(value: &str) -> Result<String> {
    let (date, time) = parse_local_datetime(value)?;
    Ok(format!("{date}T{time}:00"))
}

fn select_from_addresses(
    from: Option<Vec<EmailAddressInput>>,
    sender: Option<Vec<EmailAddressInput>>,
    account: &AuthenticatedAccount,
    account_access: &MailboxAccountAccess,
) -> Result<(EmailAddressInput, Option<EmailAddressInput>)> {
    let from = match from {
        None => EmailAddressInput {
            email: account_access.email.clone(),
            name: Some(account_access.display_name.clone()),
        },
        Some(mut addresses) => {
            if addresses.len() != 1 {
                bail!("exactly one from address is required");
            }
            let address = addresses.remove(0);
            let normalized = address.email.trim().to_lowercase();
            if normalized != account_access.email {
                bail!("from email must match the selected mailbox account");
            }
            EmailAddressInput {
                email: account_access.email.clone(),
                name: address.name,
            }
        }
    };

    let sender = match sender {
        None => None,
        Some(mut addresses) => {
            if addresses.len() != 1 {
                bail!("exactly one sender address is required");
            }
            let address = addresses.remove(0);
            let normalized = address.email.trim().to_lowercase();
            if normalized != account.email {
                bail!("sender email must match authenticated account");
            }
            Some(EmailAddressInput {
                email: account.email.clone(),
                name: address.name.or_else(|| Some(account.display_name.clone())),
            })
        }
    };

    Ok((from, sender))
}

fn map_recipients(input: Vec<EmailAddressInput>) -> Result<Vec<SubmittedRecipientInput>> {
    input
        .into_iter()
        .map(|recipient| {
            let address = recipient.email.trim().to_lowercase();
            if address.is_empty() {
                bail!("recipient email is required");
            }
            Ok(SubmittedRecipientInput {
                address,
                display_name: recipient.name.and_then(|name| {
                    let trimmed = name.trim().to_string();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed)
                    }
                }),
            })
        })
        .collect()
}

fn map_existing_recipients(recipients: &[JmapEmailAddress]) -> Vec<SubmittedRecipientInput> {
    recipients
        .iter()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
        })
        .collect()
}

fn map_parsed_recipients(
    recipients: Vec<lpe_storage::mail::ParsedMailAddress>,
) -> Vec<SubmittedRecipientInput> {
    recipients
        .into_iter()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.email,
            display_name: recipient.display_name,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use lpe_magika::{DetectionSource, Detector, MagikaDetection};
    use lpe_storage::{
        AccessibleContact, AccessibleEvent, CanonicalChangeCategory, CanonicalPushChangeSet,
        ClientContact, ClientEvent, CollaborationCollection, CollaborationRights,
        JmapImportedEmailInput, MailboxAccountAccess, SenderIdentity,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct FakeStore {
        session: Option<AuthenticatedAccount>,
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        accessible_mailbox_accounts: Vec<MailboxAccountAccess>,
        sender_identities: Vec<SenderIdentity>,
        contacts: Arc<Mutex<Vec<ClientContact>>>,
        events: Arc<Mutex<Vec<ClientEvent>>>,
        tasks: Arc<Mutex<Vec<ClientTask>>>,
        uploads: Arc<Mutex<Vec<JmapUploadBlob>>>,
        imported_emails: Arc<Mutex<Vec<JmapImportedEmailInput>>>,
        saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
        submitted_drafts: Arc<Mutex<Vec<Uuid>>>,
    }

    struct FakePushListener;

    #[derive(Clone)]
    struct FakeDetector {
        results: Arc<Mutex<Vec<Result<MagikaDetection, String>>>>,
    }

    #[test]
    fn parse_rfc822_message_collects_supported_attachment_parts() {
        let message = concat!(
            "From: Alice <alice@example.test>\r\n",
            "To: Bob <bob@example.test>\r\n",
            "Subject: Import\r\n",
            "Content-Type: multipart/mixed; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "Hello\r\n",
            "--b1\r\n",
            "Content-Type: application/vnd.oasis.opendocument.text\r\n",
            "Content-Disposition: attachment; filename=\"notes.odt\"\r\n",
            "\r\n",
            "ODT-DATA\r\n",
            "--b1--\r\n"
        );

        let parsed = parse_rfc822_message(message.as_bytes()).unwrap();

        assert_eq!(parsed.subject, "Import");
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].file_name, "notes.odt");
        assert_eq!(
            parsed.attachments[0].media_type,
            "application/vnd.oasis.opendocument.text"
        );
        assert_eq!(parsed.attachments[0].blob_bytes, b"ODT-DATA".to_vec());
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> Result<MagikaDetection> {
            self.results
                .lock()
                .unwrap()
                .remove(0)
                .map_err(anyhow::Error::msg)
        }
    }

    fn validator_ok(
        mime_type: &str,
        label: &str,
        extension: &str,
        score: f32,
    ) -> Validator<FakeDetector> {
        validator_sequence(vec![Ok(MagikaDetection {
            label: label.to_string(),
            mime_type: mime_type.to_string(),
            description: label.to_string(),
            group: "document".to_string(),
            extensions: vec![extension.to_string()],
            score: Some(score),
        })])
    }

    fn validator_sequence(
        results: Vec<Result<MagikaDetection, String>>,
    ) -> Validator<FakeDetector> {
        Validator::new(
            FakeDetector {
                results: Arc::new(Mutex::new(results)),
            },
            0.80,
        )
    }

    fn validator_error(message: &str) -> Validator<FakeDetector> {
        Validator::new(
            FakeDetector {
                results: Arc::new(Mutex::new(vec![Err(message.to_string())])),
            },
            0.80,
        )
    }

    impl JmapPushListener for FakePushListener {
        async fn wait_for_change(
            &mut self,
            _categories: &[CanonicalChangeCategory],
        ) -> Result<CanonicalPushChangeSet> {
            Ok(CanonicalPushChangeSet::default())
        }
    }

    impl FakeStore {
        fn full_rights() -> CollaborationRights {
            CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            }
        }

        fn contact_collection() -> CollaborationCollection {
            let account = Self::account();
            CollaborationCollection {
                id: "default".to_string(),
                kind: "contacts".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                display_name: "Contacts".to_string(),
                is_owned: true,
                rights: Self::full_rights(),
            }
        }

        fn calendar_collection() -> CollaborationCollection {
            let account = Self::account();
            CollaborationCollection {
                id: "default".to_string(),
                kind: "calendar".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                display_name: "Calendar".to_string(),
                is_owned: true,
                rights: Self::full_rights(),
            }
        }

        fn accessible_contact(contact: ClientContact) -> AccessibleContact {
            let account = Self::account();
            AccessibleContact {
                id: contact.id,
                collection_id: "default".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                rights: Self::full_rights(),
                name: contact.name,
                role: contact.role,
                email: contact.email,
                phone: contact.phone,
                team: contact.team,
                notes: contact.notes,
            }
        }

        fn accessible_event(event: ClientEvent) -> AccessibleEvent {
            let account = Self::account();
            AccessibleEvent {
                id: event.id,
                collection_id: "default".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                rights: Self::full_rights(),
                date: event.date,
                time: event.time,
                time_zone: event.time_zone,
                duration_minutes: event.duration_minutes,
                recurrence_rule: event.recurrence_rule,
                title: event.title,
                location: event.location,
                attendees: event.attendees,
                attendees_json: event.attendees_json,
                notes: event.notes,
            }
        }

        fn account() -> AuthenticatedAccount {
            AuthenticatedAccount {
                tenant_id: "tenant-a".to_string(),
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                email: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                expires_at: "2099-01-01T00:00:00Z".to_string(),
            }
        }

        fn draft_mailbox() -> JmapMailbox {
            JmapMailbox {
                id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
                role: "drafts".to_string(),
                name: "Drafts".to_string(),
                sort_order: 10,
                total_emails: 1,
                unread_emails: 0,
            }
        }

        fn draft_email() -> JmapEmail {
            JmapEmail {
                id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
                thread_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                mailbox_id: Self::draft_mailbox().id,
                mailbox_role: "drafts".to_string(),
                mailbox_name: "Drafts".to_string(),
                received_at: "2026-04-18T10:00:00Z".to_string(),
                sent_at: None,
                from_address: "alice@example.test".to_string(),
                from_display: Some("Alice".to_string()),
                sender_address: None,
                sender_display: None,
                sender_authorization_kind: "self".to_string(),
                submitted_by_account_id: Self::account().account_id,
                to: vec![lpe_storage::JmapEmailAddress {
                    address: "bob@example.test".to_string(),
                    display_name: Some("Bob".to_string()),
                }],
                cc: Vec::new(),
                bcc: vec![lpe_storage::JmapEmailAddress {
                    address: "hidden@example.test".to_string(),
                    display_name: None,
                }],
                subject: "Draft subject".to_string(),
                preview: "Draft preview".to_string(),
                body_text: "Draft body".to_string(),
                body_html_sanitized: None,
                unread: false,
                flagged: false,
                has_attachments: false,
                size_octets: 42,
                internet_message_id: Some("<draft@example.test>".to_string()),
                mime_blob_ref: Some(
                    "draft-message:cccccccc-cccc-cccc-cccc-cccccccccccc".to_string(),
                ),
                delivery_status: "draft".to_string(),
            }
        }

        fn inbox_mailbox() -> JmapMailbox {
            JmapMailbox {
                id: Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap(),
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                sort_order: 0,
                total_emails: 1,
                unread_emails: 1,
            }
        }

        fn inbox_email() -> JmapEmail {
            JmapEmail {
                id: Uuid::parse_str("edededed-eded-eded-eded-edededededed").unwrap(),
                thread_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                mailbox_id: Self::inbox_mailbox().id,
                mailbox_role: "inbox".to_string(),
                mailbox_name: "Inbox".to_string(),
                received_at: "2026-04-19T08:00:00Z".to_string(),
                sent_at: Some("2026-04-19T07:59:00Z".to_string()),
                from_address: "carol@example.test".to_string(),
                from_display: Some("Carol".to_string()),
                sender_address: None,
                sender_display: None,
                sender_authorization_kind: "self".to_string(),
                submitted_by_account_id: Self::account().account_id,
                to: vec![lpe_storage::JmapEmailAddress {
                    address: "alice@example.test".to_string(),
                    display_name: Some("Alice".to_string()),
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Inbox subject".to_string(),
                preview: "Inbox preview".to_string(),
                body_text: "Inbox body".to_string(),
                body_html_sanitized: Some("<p>Inbox body</p>".to_string()),
                unread: true,
                flagged: false,
                has_attachments: false,
                size_octets: 84,
                internet_message_id: Some("<inbox@example.test>".to_string()),
                mime_blob_ref: Some("upload:88888888-8888-8888-8888-888888888888".to_string()),
                delivery_status: "stored".to_string(),
            }
        }

        fn contact() -> ClientContact {
            ClientContact {
                id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
                name: "Bob Example".to_string(),
                role: "Sales".to_string(),
                email: "bob@example.test".to_string(),
                phone: "+33123456789".to_string(),
                team: "North".to_string(),
                notes: "VIP".to_string(),
            }
        }

        fn event() -> ClientEvent {
            ClientEvent {
                id: Uuid::parse_str("34343434-3434-3434-3434-343434343434").unwrap(),
                date: "2026-04-20".to_string(),
                time: "09:30".to_string(),
                time_zone: "".to_string(),
                duration_minutes: 0,
                recurrence_rule: "".to_string(),
                title: "Standup".to_string(),
                location: "Room A".to_string(),
                attendees: "bob@example.test".to_string(),
                attendees_json: serialize_calendar_participants_metadata(
                    &CalendarParticipantsMetadata {
                        organizer: Some(CalendarOrganizerMetadata {
                            email: "alice@example.test".to_string(),
                            common_name: "Alice".to_string(),
                        }),
                        attendees: vec![CalendarParticipantMetadata {
                            email: "bob@example.test".to_string(),
                            common_name: "Bob".to_string(),
                            role: "REQ-PARTICIPANT".to_string(),
                            partstat: "tentative".to_string(),
                            rsvp: true,
                        }],
                    },
                ),
                notes: "Daily sync".to_string(),
            }
        }

        fn task() -> ClientTask {
            ClientTask {
                id: Uuid::parse_str("56565656-5656-5656-5656-565656565656").unwrap(),
                title: "Prepare release".to_string(),
                description: "Confirm the release checklist".to_string(),
                status: "needs-action".to_string(),
                due_at: Some("2026-04-21T09:00:00Z".to_string()),
                completed_at: None,
                sort_order: 10,
                updated_at: "2026-04-20T15:00:00Z".to_string(),
            }
        }

        fn shared_account() -> AuthenticatedAccount {
            AuthenticatedAccount {
                tenant_id: "tenant-a".to_string(),
                account_id: Uuid::parse_str("bbbbbbbb-1111-2222-3333-444444444444").unwrap(),
                email: "shared@example.test".to_string(),
                display_name: "Shared Mailbox".to_string(),
                expires_at: "2099-01-01T00:00:00Z".to_string(),
            }
        }

        fn mailbox_access() -> MailboxAccountAccess {
            let account = Self::account();
            MailboxAccountAccess {
                account_id: account.account_id,
                email: account.email,
                display_name: account.display_name,
                is_owned: true,
                may_read: true,
                may_write: true,
                may_send_as: true,
                may_send_on_behalf: true,
            }
        }

        fn shared_mailbox_access(
            may_send_as: bool,
            may_send_on_behalf: bool,
        ) -> MailboxAccountAccess {
            let account = Self::shared_account();
            MailboxAccountAccess {
                account_id: account.account_id,
                email: account.email,
                display_name: account.display_name,
                is_owned: false,
                may_read: true,
                may_write: true,
                may_send_as,
                may_send_on_behalf,
            }
        }

        fn sender_identity() -> SenderIdentity {
            let account = Self::account();
            SenderIdentity {
                id: format!("self:{}", account.account_id),
                owner_account_id: account.account_id,
                email: account.email,
                display_name: account.display_name,
                authorization_kind: "self".to_string(),
                sender_address: None,
                sender_display: None,
            }
        }
    }

    fn push_subscription(
        enabled_types: HashSet<String>,
        last_type_states: HashMap<String, HashMap<String, String>>,
    ) -> PushSubscription {
        PushSubscription {
            enabled_types,
            last_push_state: Some(encode_push_state(&last_type_states).unwrap()),
            last_type_states,
        }
    }

    impl JmapStore for FakeStore {
        type PushListener = FakePushListener;

        async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>> {
            Ok(if token == "token" {
                self.session.clone()
            } else {
                None
            })
        }

        async fn create_push_listener(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Self::PushListener> {
            Ok(FakePushListener)
        }

        async fn fetch_jmap_mailboxes(&self, _account_id: Uuid) -> Result<Vec<JmapMailbox>> {
            Ok(self.mailboxes.clone())
        }

        async fn fetch_accessible_mailbox_accounts(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<MailboxAccountAccess>> {
            if self.accessible_mailbox_accounts.is_empty() {
                Ok(vec![Self::mailbox_access()])
            } else {
                Ok(self.accessible_mailbox_accounts.clone())
            }
        }

        async fn fetch_sender_identities(
            &self,
            _principal_account_id: Uuid,
            target_account_id: Uuid,
        ) -> Result<Vec<SenderIdentity>> {
            let identities = if self.sender_identities.is_empty() {
                vec![Self::sender_identity()]
            } else {
                self.sender_identities.clone()
            };
            Ok(identities
                .into_iter()
                .filter(|identity| identity.owner_account_id == target_account_id)
                .collect())
        }

        async fn fetch_jmap_mailbox_ids(&self, _account_id: Uuid) -> Result<Vec<Uuid>> {
            Ok(self.mailboxes.iter().map(|mailbox| mailbox.id).collect())
        }

        async fn query_jmap_email_ids(
            &self,
            _account_id: Uuid,
            mailbox_id: Option<Uuid>,
            _search_text: Option<&str>,
            position: u64,
            limit: u64,
        ) -> Result<JmapEmailQuery> {
            let mut ids = self
                .emails
                .iter()
                .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
                .map(|email| email.id)
                .collect::<Vec<_>>();
            let total = ids.len() as u64;
            ids = ids
                .into_iter()
                .skip(position as usize)
                .take(limit as usize)
                .collect();
            Ok(JmapEmailQuery { ids, total })
        }

        async fn fetch_all_jmap_email_ids(&self, _account_id: Uuid) -> Result<Vec<Uuid>> {
            Ok(self.emails.iter().map(|email| email.id).collect())
        }

        async fn fetch_all_jmap_thread_ids(&self, _account_id: Uuid) -> Result<Vec<Uuid>> {
            Ok(self
                .emails
                .iter()
                .map(|email| email.thread_id)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect())
        }

        async fn query_jmap_thread_ids(
            &self,
            _account_id: Uuid,
            mailbox_id: Option<Uuid>,
            _search_text: Option<&str>,
            position: u64,
            limit: u64,
        ) -> Result<lpe_storage::JmapThreadQuery> {
            let mut ids = self
                .emails
                .iter()
                .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
                .map(|email| email.thread_id)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            ids.sort();
            ids.reverse();
            let total = ids.len() as u64;
            ids = ids
                .into_iter()
                .skip(position as usize)
                .take(limit as usize)
                .collect();
            Ok(lpe_storage::JmapThreadQuery { ids, total })
        }

        async fn create_jmap_mailbox(
            &self,
            input: JmapMailboxCreateInput,
            _audit: AuditEntryInput,
        ) -> Result<JmapMailbox> {
            Ok(JmapMailbox {
                id: Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
                role: "".to_string(),
                name: input.name,
                sort_order: input.sort_order.unwrap_or(99),
                total_emails: 0,
                unread_emails: 0,
            })
        }

        async fn update_jmap_mailbox(
            &self,
            input: JmapMailboxUpdateInput,
            _audit: AuditEntryInput,
        ) -> Result<JmapMailbox> {
            Ok(JmapMailbox {
                id: input.mailbox_id,
                role: "".to_string(),
                name: input.name.unwrap_or_else(|| "Updated".to_string()),
                sort_order: input.sort_order.unwrap_or(10),
                total_emails: 0,
                unread_emails: 0,
            })
        }

        async fn destroy_jmap_mailbox(
            &self,
            _account_id: Uuid,
            _mailbox_id: Uuid,
            _audit: AuditEntryInput,
        ) -> Result<()> {
            Ok(())
        }

        async fn fetch_jmap_emails(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<JmapEmail>> {
            Ok(ids
                .iter()
                .filter_map(|id| self.emails.iter().find(|email| email.id == *id).cloned())
                .collect())
        }

        async fn fetch_jmap_draft(&self, _account_id: Uuid, id: Uuid) -> Result<Option<JmapEmail>> {
            Ok(self.emails.iter().find(|email| email.id == id).cloned())
        }

        async fn fetch_jmap_email_submissions(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<JmapEmailSubmission>> {
            let submissions = vec![JmapEmailSubmission {
                id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
                email_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                thread_id: FakeStore::draft_email().thread_id,
                identity_id: format!("self:{}", FakeStore::account().account_id),
                identity_email: FakeStore::account().email,
                envelope_mail_from: "alice@example.test".to_string(),
                envelope_rcpt_to: vec!["bob@example.test".to_string()],
                send_at: "2026-04-18T10:01:00Z".to_string(),
                undo_status: "final".to_string(),
                delivery_status: "queued".to_string(),
            }];
            if ids.is_empty() {
                Ok(submissions)
            } else {
                Ok(submissions
                    .into_iter()
                    .filter(|submission| ids.contains(&submission.id))
                    .collect())
            }
        }

        async fn fetch_jmap_quota(&self, _account_id: Uuid) -> Result<JmapQuota> {
            Ok(JmapQuota {
                id: "mail".to_string(),
                name: "Mail".to_string(),
                used: 10,
                hard_limit: 100,
            })
        }

        async fn save_jmap_upload_blob(
            &self,
            account_id: Uuid,
            media_type: &str,
            blob_bytes: &[u8],
        ) -> Result<JmapUploadBlob> {
            let blob = JmapUploadBlob {
                id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
                account_id,
                media_type: media_type.to_string(),
                octet_size: blob_bytes.len() as u64,
                blob_bytes: blob_bytes.to_vec(),
            };
            self.uploads.lock().unwrap().push(blob.clone());
            Ok(blob)
        }

        async fn fetch_jmap_upload_blob(
            &self,
            _account_id: Uuid,
            blob_id: Uuid,
        ) -> Result<Option<JmapUploadBlob>> {
            Ok(self
                .uploads
                .lock()
                .unwrap()
                .iter()
                .find(|blob| blob.id == blob_id)
                .cloned())
        }

        async fn save_draft_message(
            &self,
            input: SubmitMessageInput,
            _audit: AuditEntryInput,
        ) -> Result<SavedDraftMessage> {
            self.saved_drafts.lock().unwrap().push(input.clone());
            Ok(SavedDraftMessage {
                message_id: input.draft_message_id.unwrap_or_else(Uuid::new_v4),
                account_id: input.account_id,
                submitted_by_account_id: input.submitted_by_account_id,
                draft_mailbox_id: FakeStore::draft_mailbox().id,
                delivery_status: "draft".to_string(),
            })
        }

        async fn delete_draft_message(
            &self,
            _account_id: Uuid,
            _message_id: Uuid,
            _audit: AuditEntryInput,
        ) -> Result<()> {
            Ok(())
        }

        async fn submit_draft_message(
            &self,
            _account_id: Uuid,
            draft_message_id: Uuid,
            _source: &str,
            _audit: AuditEntryInput,
        ) -> Result<SubmittedMessage> {
            self.submitted_drafts.lock().unwrap().push(draft_message_id);
            Ok(SubmittedMessage {
                message_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                thread_id: FakeStore::draft_email().thread_id,
                account_id: FakeStore::account().account_id,
                submitted_by_account_id: FakeStore::account().account_id,
                sent_mailbox_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
                outbound_queue_id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
                delivery_status: "queued".to_string(),
            })
        }

        async fn copy_jmap_email(
            &self,
            _account_id: Uuid,
            _message_id: Uuid,
            target_mailbox_id: Uuid,
            _audit: AuditEntryInput,
        ) -> Result<JmapEmail> {
            let mut email = FakeStore::draft_email();
            email.id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
            email.mailbox_id = target_mailbox_id;
            email.mailbox_role = "".to_string();
            email.mailbox_name = "Archive".to_string();
            Ok(email)
        }

        async fn import_jmap_email(
            &self,
            input: JmapImportedEmailInput,
            _audit: AuditEntryInput,
        ) -> Result<JmapEmail> {
            self.imported_emails.lock().unwrap().push(input.clone());
            Ok(JmapEmail {
                id: Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap(),
                thread_id: Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap(),
                mailbox_id: input.mailbox_id,
                mailbox_role: "".to_string(),
                mailbox_name: "Imported".to_string(),
                received_at: "2026-04-18T10:05:00Z".to_string(),
                sent_at: None,
                from_address: input.from_address,
                from_display: input.from_display,
                sender_address: input.sender_address,
                sender_display: input.sender_display,
                sender_authorization_kind: "self".to_string(),
                submitted_by_account_id: input.submitted_by_account_id,
                to: input
                    .to
                    .into_iter()
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address,
                        display_name: recipient.display_name,
                    })
                    .collect(),
                cc: input
                    .cc
                    .into_iter()
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address,
                        display_name: recipient.display_name,
                    })
                    .collect(),
                bcc: Vec::new(),
                subject: input.subject,
                preview: "Imported".to_string(),
                body_text: input.body_text,
                body_html_sanitized: None,
                unread: false,
                flagged: false,
                has_attachments: false,
                size_octets: input.size_octets,
                internet_message_id: input.internet_message_id,
                mime_blob_ref: Some(input.mime_blob_ref),
                delivery_status: "stored".to_string(),
            })
        }

        async fn fetch_accessible_contact_collections(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<CollaborationCollection>> {
            Ok(vec![Self::contact_collection()])
        }

        async fn fetch_accessible_contacts(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<AccessibleContact>> {
            Ok(self
                .contacts
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .map(Self::accessible_contact)
                .collect())
        }

        async fn fetch_accessible_contacts_by_ids(
            &self,
            _principal_account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<AccessibleContact>> {
            Ok(self
                .contacts
                .lock()
                .unwrap()
                .iter()
                .filter(|contact| ids.contains(&contact.id))
                .cloned()
                .map(Self::accessible_contact)
                .collect())
        }

        async fn create_accessible_contact(
            &self,
            _principal_account_id: Uuid,
            _collection_id: Option<&str>,
            input: UpsertClientContactInput,
        ) -> Result<AccessibleContact> {
            let contact = ClientContact {
                id: input.id.unwrap_or_else(Uuid::new_v4),
                name: input.name,
                role: input.role,
                email: input.email,
                phone: input.phone,
                team: input.team,
                notes: input.notes,
            };
            let mut contacts = self.contacts.lock().unwrap();
            contacts.retain(|entry| entry.id != contact.id);
            contacts.push(contact.clone());
            Ok(Self::accessible_contact(contact))
        }

        async fn update_accessible_contact(
            &self,
            principal_account_id: Uuid,
            contact_id: Uuid,
            mut input: UpsertClientContactInput,
        ) -> Result<AccessibleContact> {
            input.id = Some(contact_id);
            self.create_accessible_contact(principal_account_id, Some("default"), input)
                .await
        }

        async fn delete_accessible_contact(
            &self,
            _principal_account_id: Uuid,
            contact_id: Uuid,
        ) -> Result<()> {
            let mut contacts = self.contacts.lock().unwrap();
            let original_len = contacts.len();
            contacts.retain(|entry| entry.id != contact_id);
            if contacts.len() == original_len {
                bail!("contact not found");
            }
            Ok(())
        }

        async fn fetch_accessible_calendar_collections(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<CollaborationCollection>> {
            Ok(vec![Self::calendar_collection()])
        }

        async fn fetch_accessible_events(
            &self,
            _principal_account_id: Uuid,
        ) -> Result<Vec<AccessibleEvent>> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .map(Self::accessible_event)
                .collect())
        }

        async fn fetch_accessible_events_by_ids(
            &self,
            _principal_account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<AccessibleEvent>> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| ids.contains(&event.id))
                .cloned()
                .map(Self::accessible_event)
                .collect())
        }

        async fn create_accessible_event(
            &self,
            _principal_account_id: Uuid,
            _collection_id: Option<&str>,
            input: UpsertClientEventInput,
        ) -> Result<AccessibleEvent> {
            let event = ClientEvent {
                id: input.id.unwrap_or_else(Uuid::new_v4),
                date: input.date,
                time: input.time,
                time_zone: input.time_zone,
                duration_minutes: input.duration_minutes,
                recurrence_rule: input.recurrence_rule,
                title: input.title,
                location: input.location,
                attendees: input.attendees,
                attendees_json: input.attendees_json,
                notes: input.notes,
            };
            let mut events = self.events.lock().unwrap();
            events.retain(|entry| entry.id != event.id);
            events.push(event.clone());
            Ok(Self::accessible_event(event))
        }

        async fn update_accessible_event(
            &self,
            principal_account_id: Uuid,
            event_id: Uuid,
            mut input: UpsertClientEventInput,
        ) -> Result<AccessibleEvent> {
            input.id = Some(event_id);
            self.create_accessible_event(principal_account_id, Some("default"), input)
                .await
        }

        async fn delete_accessible_event(
            &self,
            _principal_account_id: Uuid,
            event_id: Uuid,
        ) -> Result<()> {
            let mut events = self.events.lock().unwrap();
            let original_len = events.len();
            events.retain(|entry| entry.id != event_id);
            if events.len() == original_len {
                bail!("event not found");
            }
            Ok(())
        }

        async fn fetch_jmap_tasks(&self, _account_id: Uuid) -> Result<Vec<ClientTask>> {
            Ok(self.tasks.lock().unwrap().clone())
        }

        async fn fetch_jmap_tasks_by_ids(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<ClientTask>> {
            Ok(self
                .tasks
                .lock()
                .unwrap()
                .iter()
                .filter(|task| ids.contains(&task.id))
                .cloned()
                .collect())
        }

        async fn upsert_jmap_task(&self, input: UpsertClientTaskInput) -> Result<ClientTask> {
            let task = ClientTask {
                id: input.id.unwrap_or_else(Uuid::new_v4),
                title: input.title.trim().to_string(),
                description: input.description.trim().to_string(),
                status: input.status.trim().to_ascii_lowercase(),
                due_at: input.due_at,
                completed_at: if input.status.trim().eq_ignore_ascii_case("completed") {
                    input
                        .completed_at
                        .or_else(|| Some("2026-04-20T16:00:00Z".to_string()))
                } else {
                    None
                },
                sort_order: input.sort_order,
                updated_at: if input.id.is_some() {
                    "2026-04-20T16:00:00Z".to_string()
                } else {
                    "2026-04-20T15:30:00Z".to_string()
                },
            };
            let mut tasks = self.tasks.lock().unwrap();
            tasks.retain(|entry| entry.id != task.id);
            tasks.push(task.clone());
            Ok(task)
        }

        async fn delete_jmap_task(&self, _account_id: Uuid, task_id: Uuid) -> Result<()> {
            let mut tasks = self.tasks.lock().unwrap();
            let original_len = tasks.len();
            tasks.retain(|entry| entry.id != task_id);
            if tasks.len() == original_len {
                bail!("task not found");
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn session_uses_existing_account_authentication() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });

        let session = service
            .session_document(
                Some("Bearer token"),
                Some("wss://mail.example.test/jmap/ws"),
            )
            .await
            .unwrap();

        assert_eq!(session.username, "alice@example.test");
        assert_eq!(session.api_url, "/jmap/api");
        assert!(session.capabilities.contains_key(JMAP_MAIL_CAPABILITY));
        assert_eq!(
            session.capabilities[JMAP_WEBSOCKET_CAPABILITY]["url"],
            "wss://mail.example.test/jmap/ws"
        );
    }

    #[tokio::test]
    async fn session_and_identity_include_accessible_shared_mailbox_accounts() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(true, true),
            ],
            sender_identities: vec![
                FakeStore::sender_identity(),
                SenderIdentity {
                    id: format!("send-as:{}", FakeStore::shared_account().account_id),
                    owner_account_id: FakeStore::shared_account().account_id,
                    email: FakeStore::shared_account().email,
                    display_name: FakeStore::shared_account().display_name,
                    authorization_kind: "send-as".to_string(),
                    sender_address: None,
                    sender_display: None,
                },
                SenderIdentity {
                    id: format!("send-on-behalf:{}", FakeStore::shared_account().account_id),
                    owner_account_id: FakeStore::shared_account().account_id,
                    email: FakeStore::shared_account().email,
                    display_name: FakeStore::shared_account().display_name,
                    authorization_kind: "send-on-behalf".to_string(),
                    sender_address: Some(FakeStore::account().email),
                    sender_display: Some(FakeStore::account().display_name),
                },
            ],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let session = service
            .session_document(Some("Bearer token"), None)
            .await
            .unwrap();
        assert!(session
            .accounts
            .contains_key(&FakeStore::shared_account().account_id.to_string()));
        assert_eq!(
            session.accounts[&FakeStore::shared_account().account_id.to_string()].is_read_only,
            false
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Identity/get".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "properties": [
                                "id",
                                "email",
                                "xLpeOwnerAccountId",
                                "xLpeAuthorizationKind",
                                "xLpeSender"
                            ]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            response.method_responses[0].1["list"][1]["xLpeAuthorizationKind"],
            Value::String("send-on-behalf".to_string())
        );
        assert_eq!(
            response.method_responses[0].1["list"][1]["xLpeOwnerAccountId"],
            Value::String(FakeStore::shared_account().account_id.to_string())
        );
        assert_eq!(
            response.method_responses[0].1["list"][1]["xLpeSender"]["email"],
            Value::String("alice@example.test".to_string())
        );
    }

    #[tokio::test]
    async fn mailbox_get_projects_delegated_submit_rights_from_sender_grants() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Mailbox/get".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "ids": [FakeStore::draft_mailbox().id.to_string()],
                            "properties": ["id", "myRights"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let rights = &response.method_responses[0].1["list"][0]["myRights"];
        assert_eq!(rights["mayReadItems"], true);
        assert_eq!(rights["mayAddItems"], true);
        assert_eq!(rights["maySubmit"], false);
    }

    #[tokio::test]
    async fn email_set_creates_draft_through_canonical_storage() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "create": {
                                "k1": {
                                    "from": [{"email": "alice@example.test", "name": "Alice"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "bcc": [{"email": "hidden@example.test"}],
                                    "subject": "Hello",
                                    "textBody": "Draft body"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let saved = store.saved_drafts.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].from_address, "alice@example.test");
        assert_eq!(saved[0].bcc.len(), 1);
        assert!(response.created_ids.contains_key("k1"));
    }

    #[tokio::test]
    async fn email_set_creates_delegated_shared_mailbox_draft() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, true),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "k1": {
                                    "from": [{"email": "shared@example.test", "name": "Shared Mailbox"}],
                                    "sender": [{"email": "alice@example.test", "name": "Alice"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "subject": "Delegated",
                                    "textBody": "Shared draft"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let saved = store.saved_drafts.lock().unwrap();
        assert_eq!(saved[0].account_id, FakeStore::shared_account().account_id);
        assert_eq!(
            saved[0].submitted_by_account_id,
            FakeStore::account().account_id
        );
        assert_eq!(saved[0].from_address, "shared@example.test");
        assert_eq!(
            saved[0].sender_address.as_deref(),
            Some("alice@example.test")
        );
    }

    #[tokio::test]
    async fn email_set_rejects_inaccessible_account_id() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            accessible_mailbox_accounts: vec![FakeStore::mailbox_access()],
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "k1": {
                                    "from": [{"email": "shared@example.test"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "subject": "No access",
                                    "textBody": "Denied"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["type"],
            Value::String("invalidArguments".to_string())
        );
    }

    #[tokio::test]
    async fn email_set_maps_seen_and_flagged_keywords_to_draft_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "create": {
                                "k1": {
                                    "from": [{"email": "alice@example.test"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "subject": "Hello",
                                    "textBody": "Draft body",
                                    "keywords": {
                                        "$draft": true,
                                        "$seen": true,
                                        "$flagged": true
                                    }
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let saved = store.saved_drafts.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].unread, Some(false));
        assert_eq!(saved[0].flagged, Some(true));
    }

    #[tokio::test]
    async fn email_submission_set_submits_existing_draft_and_returns_queued_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/set".to_string(),
                        json!({
                            "create": {
                                "send1": {
                                    "emailId": FakeStore::draft_email().id.to_string()
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let submitted = store.submitted_drafts.lock().unwrap();
        assert_eq!(submitted.as_slice(), &[FakeStore::draft_email().id]);
        let payload = &response.method_responses[0].1;
        assert_eq!(
            payload["created"]["send1"]["id"],
            Value::String("11111111-2222-3333-4444-555555555555".to_string())
        );
        assert_eq!(
            payload["created"]["send1"]["undoStatus"],
            Value::String("final".to_string())
        );
    }

    #[tokio::test]
    async fn email_submission_set_rejects_delegated_submit_without_sender_grant() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/set".to_string(),
                        json!({
                            "accountId": FakeStore::shared_account().account_id.to_string(),
                            "create": {
                                "send1": {
                                    "emailId": FakeStore::draft_email().id.to_string()
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert!(store.submitted_drafts.lock().unwrap().is_empty());
        assert_eq!(
            response.method_responses[0].1["type"],
            Value::String("invalidArguments".to_string())
        );
        assert!(response.method_responses[0].1["description"]
            .as_str()
            .unwrap()
            .contains("sender delegation is required"));
    }

    #[tokio::test]
    async fn mailbox_and_email_changes_return_existing_ids_from_initial_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_CORE_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/changes".to_string(),
                            json!({"sinceState": "0"}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/changes".to_string(),
                            json!({"sinceState": "0"}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["created"][0],
            Value::String(FakeStore::draft_mailbox().id.to_string())
        );
        assert_eq!(
            response.method_responses[1].1["created"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn mailbox_and_email_query_changes_replay_snapshot_differences() {
        let initial = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::draft_mailbox()],
                emails: vec![FakeStore::draft_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall("Mailbox/query".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall("Email/query".to_string(), json!({}), "c2".to_string()),
                    ],
                },
            )
            .await
            .unwrap();

        let mailbox_query_state = initial_response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let email_query_state = initial_response.method_responses[1].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
                emails: vec![FakeStore::inbox_email(), FakeStore::draft_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/queryChanges".to_string(),
                            json!({"sinceQueryState": mailbox_query_state}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/queryChanges".to_string(),
                            json!({"sinceQueryState": email_query_state}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(response.method_responses[0].1["removed"], json!([]));
        assert_eq!(
            response.method_responses[0].1["added"][0]["id"],
            Value::String(FakeStore::inbox_mailbox().id.to_string())
        );
        assert_eq!(
            response.method_responses[1].1["added"][0]["id"],
            Value::String(FakeStore::inbox_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn email_changes_report_updates_for_existing_messages() {
        let initial = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        });
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/get".to_string(),
                        json!({}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();
        let prior_state = initial_response.method_responses[0].1["state"]
            .as_str()
            .unwrap()
            .to_string();

        let mut updated_email = FakeStore::draft_email();
        updated_email.flagged = true;
        updated_email.preview = "Updated preview".to_string();
        let updated = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            emails: vec![updated_email],
            ..Default::default()
        });
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/changes".to_string(),
                        json!({"sinceState": prior_state}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["updated"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn paged_query_states_keep_full_mailbox_and_email_snapshots() {
        let initial = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
                emails: vec![FakeStore::inbox_email(), FakeStore::draft_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let initial_response = initial
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/query".to_string(),
                            json!({"position": 0, "limit": 1}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/query".to_string(),
                            json!({"position": 0, "limit": 1}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        let mailbox_query_state = initial_response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let email_query_state = initial_response.method_responses[1].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();

        let updated = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::inbox_mailbox()],
                emails: vec![FakeStore::inbox_email()],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );
        let response = updated
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/queryChanges".to_string(),
                            json!({"sinceQueryState": mailbox_query_state}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/queryChanges".to_string(),
                            json!({"sinceQueryState": email_query_state}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["removed"][0],
            Value::String(FakeStore::draft_mailbox().id.to_string())
        );
        assert_eq!(
            response.method_responses[1].1["removed"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn identity_thread_and_submission_reads_are_available() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("Identity/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "Thread/get".to_string(),
                            json!({"ids": [FakeStore::draft_email().thread_id.to_string()]}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "EmailSubmission/get".to_string(),
                            json!({"ids": ["11111111-2222-3333-4444-555555555555"]}),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["email"],
            Value::String("alice@example.test".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["list"][0]["emailIds"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
        assert_eq!(
            response.method_responses[2].1["list"][0]["deliveryStatus"],
            Value::String("queued".to_string())
        );
    }

    #[tokio::test]
    async fn thread_query_returns_distinct_threads_for_filtered_emails() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
            emails: vec![FakeStore::inbox_email(), FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Thread/query".to_string(),
                        json!({"filter": {"inMailbox": FakeStore::inbox_mailbox().id.to_string()}}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["ids"][0],
            Value::String(FakeStore::inbox_email().thread_id.to_string())
        );
        assert_eq!(
            response.method_responses[0].1["total"],
            Value::Number(1.into())
        );
        assert!(response.method_responses[0].1["queryState"].is_string());
    }

    #[tokio::test]
    async fn thread_query_state_keeps_full_snapshot_when_page_is_limited() {
        let mut second_thread_email = FakeStore::draft_email();
        second_thread_email.thread_id =
            Uuid::parse_str("12121212-3434-5656-7878-909090909090").unwrap();
        let service = JmapService::new_with_validator(
            FakeStore {
                session: Some(FakeStore::account()),
                mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
                emails: vec![FakeStore::inbox_email(), second_thread_email],
                ..Default::default()
            },
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Thread/query".to_string(),
                        json!({"position": 0, "limit": 1}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let query_state = response.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let decoded = decode_query_state(&query_state).unwrap();

        assert_eq!(
            response.method_responses[0].1["ids"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(decoded.ids.len(), 2);
    }

    #[tokio::test]
    async fn search_snippets_return_preview_for_requested_messages() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "SearchSnippet/get".to_string(),
                        json!({"emailIds": [FakeStore::draft_email().id.to_string()]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["preview"],
            Value::String("Draft preview".to_string())
        );
    }

    #[tokio::test]
    async fn mailbox_set_copy_import_and_quota_are_available() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        store.uploads.lock().unwrap().push(JmapUploadBlob {
            id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
            account_id: FakeStore::account().account_id,
            media_type: "message/rfc822".to_string(),
            octet_size: 82,
            blob_bytes: b"From: Alice <alice@example.test>\r\nTo: Bob <bob@example.test>\r\nSubject: Imported\r\n\r\nHello".to_vec(),
        });
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/set".to_string(),
                            json!({"create": {"m1": {"name": "Archive"}}}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/copy".to_string(),
                            json!({"fromAccountId": FakeStore::account().account_id.to_string(), "create": {"e1": {"emailId": FakeStore::draft_email().id.to_string(), "mailboxIds": {"99999999-9999-9999-9999-999999999999": true}}}}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/import".to_string(),
                            json!({"emails": {"i1": {"blobId": "77777777-7777-7777-7777-777777777777", "mailboxIds": {"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": true}}}}),
                            "c3".to_string(),
                        ),
                        JmapMethodCall("Quota/get".to_string(), json!({}), "c4".to_string()),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["created"]["m1"]["id"],
            Value::String("99999999-9999-9999-9999-999999999999".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["created"]["e1"]["id"],
            Value::String("66666666-6666-6666-6666-666666666666".to_string())
        );
        assert_eq!(
            response.method_responses[2].1["created"]["i1"]["id"],
            Value::String("55555555-5555-5555-5555-555555555555".to_string())
        );
        assert_eq!(
            response.method_responses[2].1["created"]["i1"]["blobId"],
            Value::String("upload:77777777-7777-7777-7777-777777777777".to_string())
        );
        assert_eq!(
            response.method_responses[3].1["list"][0]["hardLimit"],
            Value::Number(100.into())
        );
    }

    #[tokio::test]
    async fn email_import_validates_and_preserves_multipart_attachments() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            ..Default::default()
        };
        store.uploads.lock().unwrap().push(JmapUploadBlob {
            id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
            account_id: FakeStore::account().account_id,
            media_type: "message/rfc822".to_string(),
            octet_size: 321,
            blob_bytes: concat!(
                "From: Alice <alice@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Imported\r\n",
                "Content-Type: multipart/mixed; boundary=\"b1\"\r\n",
                "\r\n",
                "--b1\r\n",
                "Content-Type: multipart/alternative; boundary=\"b2\"\r\n",
                "\r\n",
                "--b2\r\n",
                "Content-Type: text/plain\r\n",
                "\r\n",
                "Hello plain\r\n",
                "--b2\r\n",
                "Content-Type: text/html\r\n",
                "\r\n",
                "<p>Hello html</p>\r\n",
                "--b2--\r\n",
                "--b1\r\n",
                "Content-Type: application/pdf\r\n",
                "Content-Disposition: attachment; filename=\"report.pdf\"\r\n",
                "\r\n",
                "%PDF-1.7\r\n",
                "--b1--\r\n"
            )
            .as_bytes()
            .to_vec(),
        });
        let service = JmapService::new_with_validator(
            store.clone(),
            validator_sequence(vec![
                Ok(MagikaDetection {
                    label: "email".to_string(),
                    mime_type: "message/rfc822".to_string(),
                    description: "email".to_string(),
                    group: "document".to_string(),
                    extensions: vec!["eml".to_string()],
                    score: Some(0.99),
                }),
                Ok(MagikaDetection {
                    label: "pdf".to_string(),
                    mime_type: "application/pdf".to_string(),
                    description: "pdf".to_string(),
                    group: "document".to_string(),
                    extensions: vec!["pdf".to_string()],
                    score: Some(0.99),
                }),
            ]),
        );

        service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/import".to_string(),
                        json!({"emails": {"i1": {"blobId": "77777777-7777-7777-7777-777777777777", "mailboxIds": {"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": true}}}}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let imported = store.imported_emails.lock().unwrap();
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].body_text, "Hello plain");
        assert_eq!(
            imported[0].body_html_sanitized.as_deref(),
            Some("<p>Hello html</p>")
        );
        assert_eq!(imported[0].attachments.len(), 1);
        assert_eq!(imported[0].attachments[0].file_name, "report.pdf");
        assert_eq!(imported[0].attachments[0].media_type, "application/pdf");
    }

    #[tokio::test]
    async fn email_get_exposes_canonical_blob_ids_and_download_accepts_upload_prefix() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
            emails: vec![FakeStore::inbox_email(), FakeStore::draft_email()],
            uploads: Arc::new(Mutex::new(vec![JmapUploadBlob {
                id: Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap(),
                account_id: FakeStore::account().account_id,
                media_type: "message/rfc822".to_string(),
                octet_size: 9,
                blob_bytes: b"mime-body".to_vec(),
            }])),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Email/get".to_string(),
                        json!({"ids": [
                            FakeStore::inbox_email().id.to_string(),
                            FakeStore::draft_email().id.to_string()
                        ]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["blobId"],
            Value::String("upload:88888888-8888-8888-8888-888888888888".to_string())
        );
        assert_eq!(
            response.method_responses[0].1["list"][1]["blobId"],
            Value::String("draft-message:cccccccc-cccc-cccc-cccc-cccccccccccc".to_string())
        );

        let blob = service
            .handle_download(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "upload:88888888-8888-8888-8888-888888888888",
            )
            .await
            .unwrap();
        assert_eq!(blob.blob_bytes, b"mime-body".to_vec());
    }

    #[tokio::test]
    async fn session_exposes_contacts_and_calendars_capabilities() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });

        let session = service
            .session_document(
                Some("Bearer token"),
                Some("wss://mail.example.test/jmap/ws"),
            )
            .await
            .unwrap();

        assert!(session.capabilities.contains_key(JMAP_CONTACTS_CAPABILITY));
        assert!(session.capabilities.contains_key(JMAP_CALENDARS_CAPABILITY));
        assert!(session.capabilities.contains_key(JMAP_TASKS_CAPABILITY));
        assert!(session.capabilities.contains_key(JMAP_WEBSOCKET_CAPABILITY));
        assert_eq!(
            session.primary_accounts[JMAP_CONTACTS_CAPABILITY],
            FakeStore::account().account_id.to_string()
        );
        assert_eq!(
            session.primary_accounts[JMAP_CALENDARS_CAPABILITY],
            FakeStore::account().account_id.to_string()
        );
        assert_eq!(
            session.primary_accounts[JMAP_TASKS_CAPABILITY],
            FakeStore::account().account_id.to_string()
        );
    }

    #[tokio::test]
    async fn websocket_push_states_include_shared_mailbox_accounts() {
        let shared = FakeStore::shared_account();
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                MailboxAccountAccess {
                    account_id: shared.account_id,
                    email: shared.email,
                    display_name: shared.display_name,
                    is_owned: false,
                    may_read: true,
                    may_write: true,
                    may_send_as: false,
                    may_send_on_behalf: false,
                },
            ],
            ..Default::default()
        });
        let states = service
            .current_push_states(
                FakeStore::account().account_id,
                &HashSet::from([
                    "Mailbox".to_string(),
                    "AddressBook".to_string(),
                    "Task".to_string(),
                ]),
            )
            .await
            .unwrap();

        assert!(states[&FakeStore::account().account_id.to_string()].contains_key("Mailbox"));
        assert!(states[&FakeStore::account().account_id.to_string()].contains_key("AddressBook"));
        assert!(states[&FakeStore::account().account_id.to_string()].contains_key("Task"));
        assert!(states[&shared.account_id.to_string()].contains_key("Mailbox"));
        assert!(!states[&shared.account_id.to_string()].contains_key("AddressBook"));
        assert!(!states[&shared.account_id.to_string()].contains_key("Task"));
    }

    #[tokio::test]
    async fn scoped_push_change_is_stable_for_noop_mail_notifications() {
        let account = FakeStore::account();
        let enabled_types = HashSet::from(["Mailbox".to_string()]);
        let service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            ..Default::default()
        });
        let last_type_states = service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states.clone());
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Mail, [account.account_id]);

        let (changed, current_type_states) = service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert!(changed.is_empty());
        assert_eq!(current_type_states, last_type_states);
    }

    #[tokio::test]
    async fn scoped_push_change_wakes_principal_when_shared_mailbox_visibility_changes() {
        let account = FakeStore::account();
        let shared = FakeStore::shared_account();
        let enabled_types = HashSet::from(["Mailbox".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            accessible_mailbox_accounts: vec![
                FakeStore::mailbox_access(),
                FakeStore::shared_mailbox_access(false, false),
            ],
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);

        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            accessible_mailbox_accounts: vec![FakeStore::mailbox_access()],
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Mail, [shared.account_id]);

        let (changed, current_type_states) = updated_service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert!(!current_type_states.contains_key(&shared.account_id.to_string()));
        assert_eq!(
            changed[&account.account_id.to_string()]["Mailbox"],
            current_type_states[&account.account_id.to_string()]["Mailbox"]
        );
    }

    #[tokio::test]
    async fn scoped_push_change_limits_recompute_to_requested_categories() {
        let account = FakeStore::account();
        let mut updated_task = FakeStore::task();
        updated_task.updated_at = "2026-04-21T08:00:00Z".to_string();
        let enabled_types = HashSet::from(["Mailbox".to_string(), "Task".to_string()]);
        let initial_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            tasks: Arc::new(Mutex::new(vec![FakeStore::task()])),
            ..Default::default()
        });
        let last_type_states = initial_service
            .current_push_states(account.account_id, &enabled_types)
            .await
            .unwrap();
        let subscription = push_subscription(enabled_types, last_type_states);

        let updated_service = JmapService::new(FakeStore {
            session: Some(account.clone()),
            mailboxes: vec![FakeStore::inbox_mailbox()],
            tasks: Arc::new(Mutex::new(vec![updated_task])),
            ..Default::default()
        });
        let mut change_set = CanonicalPushChangeSet::default();
        change_set.insert_accounts(CanonicalChangeCategory::Tasks, [account.account_id]);

        let (changed, _) = updated_service
            .compute_push_changes(account.account_id, &subscription, &change_set)
            .await
            .unwrap();

        assert!(changed[&account.account_id.to_string()].contains_key("Task"));
        assert!(!changed[&account.account_id.to_string()].contains_key("Mailbox"));
    }

    #[tokio::test]
    async fn contacts_methods_use_canonical_contact_store() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![FakeStore::contact()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CONTACTS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("AddressBook/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "ContactCard/get".to_string(),
                            json!({"ids": [FakeStore::contact().id.to_string()]}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "ContactCard/set".to_string(),
                            json!({
                                "create": {
                                    "new1": {
                                        "name": {"full": "Carol Example"},
                                        "emails": {"main": {"address": "carol@example.test"}},
                                        "phones": {"main": {"number": "+339999"}},
                                        "organizations": {"main": {"name": "Ops"}},
                                        "titles": {"main": {"name": "Manager"}},
                                        "notes": {"main": {"note": "Priority"}},
                                        "addressBookIds": {"default": true}
                                    }
                                }
                            }),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["id"],
            Value::String("default".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["list"][0]["name"]["full"],
            Value::String("Bob Example".to_string())
        );
        assert!(response.created_ids.contains_key("new1"));
        let contacts = store.contacts.lock().unwrap();
        assert_eq!(contacts.len(), 2);
        assert!(contacts
            .iter()
            .any(|contact| contact.email == "carol@example.test"));
    }

    #[tokio::test]
    async fn calendar_methods_use_canonical_event_store() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![FakeStore::event()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CALENDARS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("Calendar/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "CalendarEvent/query".to_string(),
                            json!({"filter": {"inCalendar": "default"}}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "CalendarEvent/set".to_string(),
                            json!({
                                "create": {
                                    "ev1": {
                                        "@type": "Event",
                                        "title": "Planning",
                                        "start": "2026-04-21T11:00:00",
                                        "duration": "PT0S",
                                        "locations": {"main": {"name": "Room B"}},
                                        "participants": {
                                            "owner": {
                                                "name": "Alice",
                                                "email": "alice@example.test",
                                                "roles": {"owner": true}
                                            },
                                            "p1": {
                                                "name": "Bob",
                                                "email": "bob@example.test",
                                                "roles": {"attendee": true},
                                                "participationStatus": "accepted",
                                                "expectReply": true
                                            }
                                        },
                                        "description": "Weekly planning",
                                        "calendarIds": {"default": true}
                                    }
                                }
                            }),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["id"],
            Value::String("default".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["ids"][0],
            Value::String(FakeStore::event().id.to_string())
        );
        let events = store.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|event| event.title == "Planning"));
        let created = events
            .iter()
            .find(|event| event.title == "Planning")
            .unwrap();
        assert_eq!(created.attendees, "Bob");
        assert!(created.attendees_json.contains("\"organizer\""));
        assert!(created.attendees_json.contains("\"partstat\":\"accepted\""));
        assert!(created.attendees_json.contains("\"rsvp\":true"));
    }

    #[tokio::test]
    async fn calendar_event_get_exposes_owner_and_participation_status() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![FakeStore::event()])),
            ..Default::default()
        };
        let service = JmapService::new(store);

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CALENDARS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "CalendarEvent/get".to_string(),
                        json!({
                            "ids": [FakeStore::event().id.to_string()],
                            "properties": ["id", "participants"]
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let event = &response.method_responses[0].1["list"][0];
        assert_eq!(
            event["participants"]["owner"]["email"],
            "alice@example.test"
        );
        assert_eq!(event["participants"]["owner"]["roles"]["owner"], true);
        assert_eq!(event["participants"]["p1"]["email"], "bob@example.test");
        assert_eq!(
            event["participants"]["p1"]["participationStatus"],
            "tentative"
        );
        assert_eq!(event["participants"]["p1"]["expectReply"], true);
    }

    #[tokio::test]
    async fn task_methods_use_canonical_task_store() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_TASKS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("TaskList/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "Task/get".to_string(),
                            json!({"ids": [FakeStore::task().id.to_string()]}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "Task/set".to_string(),
                            json!({
                                "create": {
                                    "t1": {
                                        "@type": "Task",
                                        "title": "Follow up",
                                        "description": "Send customer recap",
                                        "status": "in-progress",
                                        "due": "2026-04-22T08:30:00Z",
                                        "sortOrder": 20,
                                        "taskListId": "default"
                                    }
                                }
                            }),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["id"],
            Value::String("default".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["list"][0]["status"],
            Value::String("needs-action".to_string())
        );
        assert!(response.created_ids.contains_key("t1"));
        let tasks = store.tasks.lock().unwrap();
        assert_eq!(tasks.len(), 2);
        assert!(tasks.iter().any(|task| task.title == "Follow up"));
    }

    #[tokio::test]
    async fn task_query_changes_tracks_sort_order_and_updates() {
        let mut second_task = FakeStore::task();
        second_task.id = Uuid::parse_str("67676767-6767-6767-6767-676767676767").unwrap();
        second_task.title = "Review notes".to_string();
        second_task.sort_order = 20;
        second_task.updated_at = "2026-04-20T15:10:00Z".to_string();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task(), second_task.clone()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let initial = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Task/query".to_string(),
                            json!({"sort": [{"property": "sortOrder", "isAscending": true}]}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall("Task/get".to_string(), json!({}), "c2".to_string()),
                    ],
                },
            )
            .await
            .unwrap();

        let query_state = initial.method_responses[0].1["queryState"]
            .as_str()
            .unwrap()
            .to_string();
        let task_state = initial.method_responses[1].1["state"]
            .as_str()
            .unwrap()
            .to_string();

        service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "Task/set".to_string(),
                        json!({
                            "update": {
                                second_task.id.to_string(): {
                                    "title": "Review notes",
                                    "description": "Review architecture notes",
                                    "status": "completed",
                                    "completed": "2026-04-20T16:00:00Z",
                                    "sortOrder": 5,
                                    "taskListId": "default"
                                }
                            }
                        }),
                        "c2".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let changes = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_TASKS_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Task/queryChanges".to_string(),
                            json!({
                                "sinceQueryState": query_state,
                                "sort": [{"property": "sortOrder", "isAscending": true}]
                            }),
                            "c3".to_string(),
                        ),
                        JmapMethodCall(
                            "Task/changes".to_string(),
                            json!({
                                "sinceState": task_state
                            }),
                            "c4".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert!(changes.method_responses[0].1["removed"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == &Value::String(second_task.id.to_string())));
        assert!(changes.method_responses[0].1["added"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value["id"] == Value::String(second_task.id.to_string())));
        assert!(changes.method_responses[1].1["updated"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == &Value::String(second_task.id.to_string())));
    }

    #[tokio::test]
    async fn upload_and_download_use_authenticated_account() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store.clone(),
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let upload = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap();
        assert_eq!(
            upload["blobId"],
            Value::String("77777777-7777-7777-7777-777777777777".to_string())
        );

        let blob = service
            .handle_download(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "77777777-7777-7777-7777-777777777777",
            )
            .await
            .unwrap();
        assert_eq!(blob.media_type, "message/rfc822");
        assert_eq!(blob.blob_bytes, b"Subject: Hello\r\n\r\nBody".to_vec());
    }

    #[tokio::test]
    async fn upload_accepts_validated_matching_blob() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let upload = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap();

        assert_eq!(upload["type"], Value::String("message/rfc822".to_string()));
    }

    #[tokio::test]
    async fn upload_rejects_declared_mime_mismatch() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("application/pdf", "pdf", "pdf", 0.99),
        );

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"%PDF-1.7",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("JMAP upload blocked"));
    }

    #[tokio::test]
    async fn upload_rejects_unknown_type() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            Validator::new(
                FakeDetector {
                    results: Arc::new(Mutex::new(vec![Ok(MagikaDetection {
                        label: "unknown_binary".to_string(),
                        mime_type: "application/octet-stream".to_string(),
                        description: "unknown".to_string(),
                        group: "unknown".to_string(),
                        extensions: Vec::new(),
                        score: Some(0.99),
                    })])),
                },
                0.80,
            ),
        );

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "application/octet-stream",
                b"\x00\x01\x02",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("JMAP upload blocked"));
    }

    #[tokio::test]
    async fn upload_surfaces_magika_failure_mode() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service =
            JmapService::new_with_validator(store, validator_error("Magika command failed"));

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Magika command failed"));
    }
}
