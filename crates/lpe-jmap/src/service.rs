use anyhow::{anyhow, bail, Result};
use axum::{
    body::{Body, Bytes},
    extract::{ws::WebSocketUpgrade, DefaultBodyLimit, Query, State},
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AuditEntryInput, AuthenticatedAccount, ClientTask,
    ClientTaskList, CollaborationCollection, JmapEmail, JmapEmailSubmission, JmapMailbox,
    JmapUploadBlob, MailboxAccountAccess, MailboxRule, OutlookProfileState, SearchFolderDefinition,
    SenderIdentity, Storage, UpsertSearchFolderInput,
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::{Arc, OnceLock},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::{
    convert::format_addresses,
    error::{
        http_error, jmap_problem, method_error, method_error_from_error, set_error,
        JMAP_PROBLEM_LIMIT, JMAP_PROBLEM_UNKNOWN_CAPABILITY,
    },
    eventsource::EventSourceQuery,
    parse::parse_uuid,
    protocol::{
        JmapApiRequest, JmapApiResponse, JmapMethodCall, JmapMethodResponse, SessionDocument,
    },
    session,
    state::{
        changes_response_from_durable_with_cursor, changes_response_with_cursor,
        decode_query_state, encode_query_state, encode_query_state_reference, encode_state,
        encode_state_with_cursor, query_changes_response_from_diff, query_diff_for_kind,
        query_position, state_cursor, validate_query_state_token, DurableObjectChange, StateEntry,
    },
    store::{JmapShareInput, JmapStore},
    upload::{message_rfc822_bytes, JmapBlobId},
};

mod blobs;
mod canonical;
mod helpers;
mod state_objects;
use helpers::*;
pub(crate) use helpers::{collection_state_fingerprint, opaque_state_fingerprint, trim_snippet};

pub(crate) const JMAP_CORE_CAPABILITY: &str = "urn:ietf:params:jmap:core";
pub(crate) const JMAP_MAIL_CAPABILITY: &str = "urn:ietf:params:jmap:mail";
pub(crate) const JMAP_SUBMISSION_CAPABILITY: &str = "urn:ietf:params:jmap:submission";
pub(crate) const JMAP_BLOB_CAPABILITY: &str = "urn:ietf:params:jmap:blob";
pub(crate) const JMAP_CONTACTS_CAPABILITY: &str = "urn:ietf:params:jmap:contacts";
pub(crate) const JMAP_CALENDARS_CAPABILITY: &str = "urn:ietf:params:jmap:calendars";
pub(crate) const JMAP_TASKS_CAPABILITY: &str = "urn:ietf:params:jmap:tasks";
pub(crate) const JMAP_LPE_OUTLOOK_CAPABILITY: &str = "https://l-p-e.ch/jmap/outlook";
pub(crate) const JMAP_VACATION_RESPONSE_CAPABILITY: &str = "urn:ietf:params:jmap:vacationresponse";
pub(crate) const JMAP_WEBSOCKET_CAPABILITY: &str = "urn:ietf:params:jmap:websocket";
pub(crate) const SESSION_STATE: &str = "mvp-3";
pub(crate) const QUERY_STATE_VERSION: &str = "mvp-3";
pub(crate) const STATE_TOKEN_VERSION: &str = "mvp-2";
pub(crate) const PUSH_STATE_VERSION: &str = "mvp-push-1";
pub(crate) const MAX_CALLS_IN_REQUEST: u64 = 16;
pub(crate) const MAX_QUERY_LIMIT: u64 = 250;
pub(crate) const DEFAULT_GET_LIMIT: u64 = 100;
pub(crate) const MAX_SIZE_REQUEST: u64 = 10 * 1024 * 1024;
pub(crate) const MAX_SIZE_UPLOAD: u64 = 25 * 1024 * 1024;
pub(crate) const MAX_CONCURRENT_REQUESTS: u64 = 4;
pub(crate) const MAX_CONCURRENT_UPLOAD: u64 = 4;
pub(crate) const MAX_OBJECTS_IN_GET: u64 = 250;
pub(crate) const MAX_OBJECTS_IN_SET: u64 = 128;
pub(crate) const MAX_BLOB_DATA_SOURCES: u64 = 64;

type HttpResult<T> = std::result::Result<Json<T>, (StatusCode, Json<Value>)>;
static API_REQUEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
static UPLOAD_REQUEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

pub fn router() -> Router<Storage> {
    Router::new()
        .route("/session", get(session_handler))
        .route(
            "/api",
            post(api_handler)
                .layer::<_, Infallible>(middleware::from_fn(api_concurrency_limit))
                .layer(DefaultBodyLimit::max(MAX_SIZE_REQUEST as usize)),
        )
        .route("/ws", get(websocket_handler))
        .route("/events", get(event_source_handler))
        .route(
            "/upload/{account_id}",
            post(upload_handler)
                .layer::<_, Infallible>(middleware::from_fn(upload_concurrency_limit)),
        )
        .route(
            "/download/{account_id}/{blob_id}/{name}",
            get(download_handler),
        )
}

#[derive(Clone)]
pub struct JmapService<S, V = lpe_magika::SystemDetector> {
    pub(crate) store: S,
    pub(crate) validator: Validator<V>,
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
    let websocket_url = session::websocket_url(&headers);
    let public_base_url =
        session::public_base_url(&headers).unwrap_or_else(|| session::public_base_path(&headers));
    Ok(Json(
        service
            .session_document(
                authorization.as_deref(),
                websocket_url.as_deref(),
                Some(&public_base_url),
            )
            .await
            .map_err(http_error)?,
    ))
}

async fn api_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<JmapApiRequest>,
) -> HttpResult<JmapApiResponse> {
    if api_request_exceeds_call_limit(&request) {
        return Err(jmap_problem(
            JMAP_PROBLEM_LIMIT,
            StatusCode::PAYLOAD_TOO_LARGE,
            "JMAP request exceeds maxCallsInRequest",
            Some("maxCallsInRequest"),
        ));
    }
    if let Err(error) = validate_declared_capabilities(&request) {
        return Err(jmap_problem(
            JMAP_PROBLEM_UNKNOWN_CAPABILITY,
            StatusCode::BAD_REQUEST,
            error.to_string(),
            None,
        ));
    }
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    Ok(Json(
        service
            .handle_api_request(authorization.as_deref(), request)
            .await
            .map_err(http_error)?,
    ))
}

async fn api_concurrency_limit(request: Request<Body>, next: Next) -> Response {
    let Some(_permit) = try_acquire_api_request_permit() else {
        return jmap_problem(
            JMAP_PROBLEM_LIMIT,
            StatusCode::TOO_MANY_REQUESTS,
            "JMAP request exceeds maxConcurrentRequests",
            Some("maxConcurrentRequests"),
        )
        .into_response();
    };
    next.run(request).await
}

pub(crate) fn try_acquire_api_request_permit() -> Option<OwnedSemaphorePermit> {
    API_REQUEST_SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS as usize)))
        .clone()
        .try_acquire_owned()
        .ok()
}

async fn upload_concurrency_limit(request: Request<Body>, next: Next) -> Response {
    let Some(_permit) = try_acquire_upload_request_permit() else {
        return jmap_problem(
            JMAP_PROBLEM_LIMIT,
            StatusCode::TOO_MANY_REQUESTS,
            "JMAP upload exceeds maxConcurrentUpload",
            Some("maxConcurrentUpload"),
        )
        .into_response();
    };
    next.run(request).await
}

pub(crate) fn try_acquire_upload_request_permit() -> Option<OwnedSemaphorePermit> {
    UPLOAD_REQUEST_SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOAD as usize)))
        .clone()
        .try_acquire_owned()
        .ok()
}

async fn upload_handler(
    State(storage): State<Storage>,
    axum::extract::Path(account_id): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<Value>)> {
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
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<Value>)> {
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
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<Value>)> {
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

async fn event_source_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Query(query): Query<EventSourceQuery>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let account = service
        .authenticate(authorization.as_deref())
        .await
        .map_err(http_error)?;
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    service
        .handle_event_source(account, query, last_event_id)
        .await
        .map_err(http_error)
}

impl<S: JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn requested_account_access(
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

    pub async fn handle_api_request(
        &self,
        authorization: Option<&str>,
        request: JmapApiRequest,
    ) -> Result<JmapApiResponse> {
        let account = self.authenticate(authorization).await?;
        self.handle_api_request_for_account(&account, request).await
    }

    pub(crate) async fn handle_api_request_for_account(
        &self,
        account: &AuthenticatedAccount,
        request: JmapApiRequest,
    ) -> Result<JmapApiResponse> {
        if api_request_exceeds_call_limit(&request) {
            bail!("JMAP request exceeds maxCallsInRequest");
        }
        validate_declared_capabilities(&request)?;
        let declared_capabilities = request.using_capabilities;
        let mut method_responses = Vec::with_capacity(request.method_calls.len());
        let mut created_ids = HashMap::new();
        let mut previous_results: HashMap<String, (String, Value)> = HashMap::new();

        for JmapMethodCall(method_name, arguments, call_id) in request.method_calls {
            let arguments = match resolve_result_references(arguments, &previous_results) {
                Ok(arguments) => arguments,
                Err(payload) => {
                    let response_name = "error".to_string();
                    previous_results
                        .insert(call_id.clone(), (response_name.clone(), payload.clone()));
                    method_responses.push(JmapMethodResponse(response_name, payload, call_id));
                    continue;
                }
            };
            let response = if method_capability(method_name.as_str())
                .map(|capability| {
                    declared_capabilities
                        .iter()
                        .any(|declared| declared == capability)
                })
                .unwrap_or(true)
            {
                if let Some(error) = method_object_limit_error(&method_name, &arguments) {
                    Ok(error)
                } else {
                    match method_name.as_str() {
                        "Mailbox/get" => self.handle_mailbox_get(account, arguments).await,
                        "Mailbox/query" => self.handle_mailbox_query(account, arguments).await,
                        "Mailbox/queryChanges" => {
                            self.handle_mailbox_query_changes(account, arguments).await
                        }
                        "Mailbox/changes" => self.handle_mailbox_changes(account, arguments).await,
                        "Mailbox/set" => {
                            self.handle_mailbox_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "Mailbox/import" | "Mailbox/copy" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "Mailbox",
                                &method_name,
                            )
                            .await
                        }
                        "Email/query" => self.handle_email_query(account, arguments).await,
                        "Email/queryChanges" => {
                            self.handle_email_query_changes(account, arguments).await
                        }
                        "Email/get" => self.handle_email_get(account, arguments).await,
                        "Email/changes" => self.handle_email_changes(account, arguments).await,
                        "Email/set" => {
                            self.handle_email_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "Email/copy" => {
                            self.handle_email_copy(account, arguments, &mut created_ids)
                                .await
                        }
                        "Email/import" => {
                            self.handle_email_import(account, arguments, &mut created_ids)
                                .await
                        }
                        "EmailSubmission/get" => {
                            self.handle_email_submission_get(account, arguments).await
                        }
                        "EmailSubmission/changes" => {
                            self.handle_email_submission_changes(account, arguments)
                                .await
                        }
                        "EmailSubmission/query" => {
                            self.handle_email_submission_query(account, arguments).await
                        }
                        "EmailSubmission/queryChanges" => {
                            self.handle_email_submission_query_changes(account, arguments)
                                .await
                        }
                        "EmailSubmission/set" => {
                            self.handle_email_submission_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "EmailSubmission/import" | "EmailSubmission/copy" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "EmailSubmission",
                                &method_name,
                            )
                            .await
                        }
                        "AddressBook/get" => self.handle_address_book_get(account, arguments).await,
                        "AddressBook/query" => {
                            self.handle_address_book_query(account, arguments).await
                        }
                        "AddressBook/queryChanges" => {
                            self.handle_address_book_query_changes(account, arguments)
                                .await
                        }
                        "AddressBook/changes" => {
                            self.handle_address_book_changes(account, arguments).await
                        }
                        "ContactCard/get" => self.handle_contact_get(account, arguments).await,
                        "ContactCard/query" => self.handle_contact_query(account, arguments).await,
                        "ContactCard/queryChanges" => {
                            self.handle_contact_query_changes(account, arguments).await
                        }
                        "ContactCard/changes" => {
                            self.handle_contact_changes(account, arguments).await
                        }
                        "ContactCard/set" => {
                            self.handle_contact_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "RecipientSuggestion/query" => {
                            self.handle_recipient_suggestion_query(account, arguments)
                                .await
                        }
                        "Calendar/get" => self.handle_calendar_get(account, arguments).await,
                        "Calendar/query" => self.handle_calendar_query(account, arguments).await,
                        "Calendar/queryChanges" => {
                            self.handle_calendar_query_changes(account, arguments).await
                        }
                        "Calendar/changes" => {
                            self.handle_calendar_changes(account, arguments).await
                        }
                        "Calendar/set" => {
                            self.handle_calendar_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "Calendar/import" | "Calendar/copy" => {
                            self.handle_calendar_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                            )
                            .await
                        }
                        "CalendarEvent/get" => {
                            self.handle_calendar_event_get(account, arguments).await
                        }
                        "CalendarEvent/query" => {
                            self.handle_calendar_event_query(account, arguments).await
                        }
                        "CalendarEvent/queryChanges" => {
                            self.handle_calendar_event_query_changes(account, arguments)
                                .await
                        }
                        "CalendarEvent/changes" => {
                            self.handle_calendar_event_changes(account, arguments).await
                        }
                        "CalendarEvent/set" => {
                            self.handle_calendar_event_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "TaskList/get" => self.handle_task_list_get(account, arguments).await,
                        "TaskList/changes" => {
                            self.handle_task_list_changes(account, arguments).await
                        }
                        "TaskList/set" => {
                            self.handle_task_list_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "TaskList/query" => {
                            self.handle_canonical_query(account, arguments, "TaskList")
                                .await
                        }
                        "TaskList/queryChanges" => {
                            self.handle_canonical_query_changes(account, arguments, "TaskList")
                                .await
                        }
                        "TaskList/import" | "TaskList/copy" => {
                            self.handle_canonical_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                                "TaskList",
                                &method_name,
                            )
                            .await
                        }
                        "Task/get" => self.handle_task_get(account, arguments).await,
                        "Task/query" => self.handle_task_query(account, arguments).await,
                        "Task/queryChanges" => {
                            self.handle_task_query_changes(account, arguments).await
                        }
                        "Task/changes" => self.handle_task_changes(account, arguments).await,
                        "Task/set" => {
                            self.handle_task_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "Task/import" | "Task/copy" => {
                            self.handle_canonical_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                                "Task",
                                &method_name,
                            )
                            .await
                        }
                        "Note/get" => self.handle_note_get(account, arguments).await,
                        "Note/query" => self.handle_note_query(account, arguments).await,
                        "Note/queryChanges" => {
                            self.handle_note_query_changes(account, arguments).await
                        }
                        "Note/changes" => self.handle_note_changes(account, arguments).await,
                        "Note/set" => {
                            self.handle_note_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "Note/import" | "Note/copy" => {
                            self.handle_canonical_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                                "Note",
                                &method_name,
                            )
                            .await
                        }
                        "JournalEntry/get" => {
                            self.handle_journal_entry_get(account, arguments).await
                        }
                        "JournalEntry/query" => {
                            self.handle_journal_entry_query(account, arguments).await
                        }
                        "JournalEntry/queryChanges" => {
                            self.handle_journal_entry_query_changes(account, arguments)
                                .await
                        }
                        "JournalEntry/changes" => {
                            self.handle_journal_entry_changes(account, arguments).await
                        }
                        "JournalEntry/set" => {
                            self.handle_journal_entry_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "JournalEntry/import" | "JournalEntry/copy" => {
                            self.handle_canonical_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                                "JournalEntry",
                                &method_name,
                            )
                            .await
                        }
                        "Reminder/query" => self.handle_reminder_query(account, arguments).await,
                        "Reminder/get" => {
                            self.handle_canonical_get(account, arguments, "Reminder")
                                .await
                        }
                        "Reminder/changes" => {
                            self.handle_canonical_changes(account, arguments, "Reminder")
                                .await
                        }
                        "Reminder/queryChanges" => {
                            self.handle_canonical_query_changes(account, arguments, "Reminder")
                                .await
                        }
                        "Reminder/set" => {
                            self.handle_reminder_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "Reminder/import" | "Reminder/copy" => {
                            self.handle_reminder_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                                &method_name,
                            )
                            .await
                        }
                        "Rule/get" => self.handle_canonical_get(account, arguments, "Rule").await,
                        "Rule/query" => {
                            self.handle_canonical_query(account, arguments, "Rule")
                                .await
                        }
                        "Rule/changes" => {
                            self.handle_canonical_changes(account, arguments, "Rule")
                                .await
                        }
                        "Rule/queryChanges" => {
                            self.handle_canonical_query_changes(account, arguments, "Rule")
                                .await
                        }
                        "Rule/set" | "Rule/import" | "Rule/copy" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "Rule",
                                &method_name,
                            )
                            .await
                        }
                        "OutlookProfile/get" => {
                            self.handle_canonical_get(account, arguments, "OutlookProfile")
                                .await
                        }
                        "OutlookProfile/query" => {
                            self.handle_canonical_query(account, arguments, "OutlookProfile")
                                .await
                        }
                        "OutlookProfile/changes" => {
                            self.handle_canonical_changes(account, arguments, "OutlookProfile")
                                .await
                        }
                        "OutlookProfile/queryChanges" => {
                            self.handle_canonical_query_changes(
                                account,
                                arguments,
                                "OutlookProfile",
                            )
                            .await
                        }
                        "OutlookProfile/set" | "OutlookProfile/import" | "OutlookProfile/copy" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "OutlookProfile",
                                &method_name,
                            )
                            .await
                        }
                        "SearchFolder/get" => {
                            self.handle_canonical_get(account, arguments, "SearchFolder")
                                .await
                        }
                        "SearchFolder/query" => {
                            self.handle_canonical_query(account, arguments, "SearchFolder")
                                .await
                        }
                        "SearchFolder/changes" => {
                            self.handle_canonical_changes(account, arguments, "SearchFolder")
                                .await
                        }
                        "SearchFolder/queryChanges" => {
                            self.handle_canonical_query_changes(account, arguments, "SearchFolder")
                                .await
                        }
                        "SearchFolder/set" => {
                            self.handle_search_folder_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "SearchFolder/import" | "SearchFolder/copy" => {
                            self.handle_search_folder_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                                &method_name,
                            )
                            .await
                        }
                        "Identity/get" => self.handle_identity_get(account, arguments).await,
                        "Identity/changes" => {
                            self.handle_identity_changes(account, arguments).await
                        }
                        "Identity/query" => {
                            self.handle_canonical_query(account, arguments, "Identity")
                                .await
                        }
                        "Identity/queryChanges" => {
                            self.handle_canonical_query_changes(account, arguments, "Identity")
                                .await
                        }
                        "Identity/set" | "Identity/import" | "Identity/copy" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "Identity",
                                &method_name,
                            )
                            .await
                        }
                        "Thread/query" => self.handle_thread_query(account, arguments).await,
                        "Thread/queryChanges" => {
                            self.handle_thread_query_changes(account, arguments).await
                        }
                        "Thread/get" => self.handle_thread_get(account, arguments).await,
                        "Thread/changes" => self.handle_thread_changes(account, arguments).await,
                        "Thread/set" | "Thread/import" | "Thread/copy" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "Thread",
                                &method_name,
                            )
                            .await
                        }
                        "Quota/get" => self.handle_quota_get(account, arguments).await,
                        "SearchSnippet/get" => {
                            self.handle_search_snippet_get(account, arguments).await
                        }
                        "Blob/upload" => {
                            self.handle_blob_upload(account, arguments, &mut created_ids)
                                .await
                        }
                        "Blob/get" => self.handle_blob_get(account, arguments, &created_ids).await,
                        "Blob/query" => {
                            self.handle_canonical_query(account, arguments, "Blob")
                                .await
                        }
                        "Blob/changes" => {
                            self.handle_canonical_changes(account, arguments, "Blob")
                                .await
                        }
                        "Blob/queryChanges" => {
                            self.handle_canonical_query_changes(account, arguments, "Blob")
                                .await
                        }
                        "Blob/set" | "Blob/import" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "Blob",
                                &method_name,
                            )
                            .await
                        }
                        "Blob/lookup" => {
                            self.handle_blob_lookup(
                                account,
                                arguments,
                                &created_ids,
                                &declared_capabilities,
                            )
                            .await
                        }
                        "Blob/copy" => {
                            self.handle_blob_copy(account, arguments, &created_ids)
                                .await
                        }
                        "AddressBook/set" | "AddressBook/import" | "AddressBook/copy" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "AddressBook",
                                &method_name,
                            )
                            .await
                        }
                        "ContactCard/import" | "ContactCard/copy" => {
                            self.handle_canonical_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                                "ContactCard",
                                &method_name,
                            )
                            .await
                        }
                        "CalendarEvent/import" | "CalendarEvent/copy" => {
                            self.handle_canonical_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                                "CalendarEvent",
                                &method_name,
                            )
                            .await
                        }
                        "Share/get" => self.handle_canonical_get(account, arguments, "Share").await,
                        "Share/query" => {
                            self.handle_canonical_query(account, arguments, "Share")
                                .await
                        }
                        "Share/changes" => {
                            self.handle_canonical_changes(account, arguments, "Share")
                                .await
                        }
                        "Share/queryChanges" => {
                            self.handle_canonical_query_changes(account, arguments, "Share")
                                .await
                        }
                        "Share/set" => {
                            self.handle_share_set(account, arguments, &mut created_ids)
                                .await
                        }
                        "Share/import" | "Share/copy" => {
                            self.handle_share_import_or_copy(
                                account,
                                arguments,
                                &mut created_ids,
                                &method_name,
                            )
                            .await
                        }
                        "DurableChange/get" => {
                            self.handle_canonical_get(account, arguments, "DurableChange")
                                .await
                        }
                        "DurableChange/query" => {
                            self.handle_canonical_query(account, arguments, "DurableChange")
                                .await
                        }
                        "DurableChange/changes" => {
                            self.handle_canonical_changes(account, arguments, "DurableChange")
                                .await
                        }
                        "DurableChange/queryChanges" => {
                            self.handle_canonical_query_changes(account, arguments, "DurableChange")
                                .await
                        }
                        "DurableChange/set" | "DurableChange/import" | "DurableChange/copy" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "DurableChange",
                                &method_name,
                            )
                            .await
                        }
                        "VacationResponse/get" => {
                            self.handle_vacation_response_get(account, arguments).await
                        }
                        "VacationResponse/set" => {
                            self.handle_vacation_response_set(account, arguments, &mut created_ids)
                                .await
                        }
                        _ => Ok(method_error("unknownMethod", "method is not supported")),
                    }
                }
            } else {
                Ok(method_error(
                    "unknownMethod",
                    "method capability is not requested",
                ))
            };

            let payload = match response {
                Ok(payload) => payload,
                Err(error) => method_error_from_error(error),
            };
            let response_name = if is_method_error_payload(&payload) {
                "error".to_string()
            } else {
                method_name.clone()
            };
            previous_results.insert(call_id.clone(), (response_name.clone(), payload.clone()));
            method_responses.push(JmapMethodResponse(response_name, payload, call_id));
        }

        let accessible_accounts = self
            .store
            .fetch_accessible_mailbox_accounts(account.account_id)
            .await?;

        Ok(JmapApiResponse {
            method_responses,
            created_ids,
            session_state: session::session_state(&accessible_accounts),
        })
    }

    pub(crate) async fn handle_reminder_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let old_state = self
            .canonical_object_state(account, account_id, "Reminder")
            .await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.get("create").and_then(Value::as_object) {
            for (creation_id, value) in create {
                match self
                    .apply_reminder_mutation(account, account_id, value, true, creation_id)
                    .await
                {
                    Ok(id) => {
                        created_ids.insert(creation_id.clone(), id.clone());
                        created.insert(creation_id.clone(), json!({"id": id}));
                    }
                    Err(error) => {
                        not_created.insert(creation_id.clone(), set_error(&error.to_string()));
                    }
                }
            }
        }
        if let Some(update) = arguments.get("update").and_then(Value::as_object) {
            for (id, value) in update {
                let mut object = value.clone();
                if let Value::Object(map) = &mut object {
                    let (source_type, source_id, occurrence_start_at) = parse_reminder_id(id)?;
                    map.entry("sourceType")
                        .or_insert_with(|| Value::String(source_type));
                    map.entry("sourceId")
                        .or_insert_with(|| Value::String(source_id.to_string()));
                    if let Some(occurrence_start_at) = occurrence_start_at {
                        map.entry("occurrenceStartAt")
                            .or_insert_with(|| Value::String(occurrence_start_at));
                    }
                }
                match self
                    .apply_reminder_mutation(account, account_id, &object, true, id)
                    .await
                {
                    Ok(_) => {
                        updated.insert(id.clone(), Value::Object(Map::new()));
                    }
                    Err(error) => {
                        not_updated.insert(id.clone(), set_error(&error.to_string()));
                    }
                }
            }
        }
        if let Some(ids) = arguments.get("destroy").and_then(Value::as_array) {
            for value in ids {
                let Some(id) = value.as_str() else {
                    continue;
                };
                let (source_type, source_id, occurrence_start_at) = match parse_reminder_id(id) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        not_destroyed.insert(id.to_string(), set_error(&error.to_string()));
                        continue;
                    }
                };
                let object = json!({
                    "sourceType": source_type,
                    "sourceId": source_id.to_string(),
                    "reminderSet": false,
                });
                let mut object = object;
                if let (Some(occurrence_start_at), Value::Object(map)) =
                    (occurrence_start_at, &mut object)
                {
                    map.insert(
                        "occurrenceStartAt".to_string(),
                        Value::String(occurrence_start_at),
                    );
                }
                match self
                    .apply_reminder_mutation(account, account_id, &object, false, id)
                    .await
                {
                    Ok(_) => destroyed.push(Value::String(id.to_string())),
                    Err(error) => {
                        not_destroyed.insert(id.to_string(), set_error(&error.to_string()));
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": self.canonical_object_state(account, account_id, "Reminder").await?,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    pub(crate) async fn handle_reminder_import_or_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
        method_name: &str,
    ) -> Result<Value> {
        let mut set_arguments = Map::new();
        if let Some(account_id) = arguments.get("accountId").cloned() {
            set_arguments.insert("accountId".to_string(), account_id);
        }
        let create = arguments
            .get("create")
            .or_else(|| arguments.get("emails"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        set_arguments.insert("create".to_string(), create);
        let mut response = self
            .handle_reminder_set(account, Value::Object(set_arguments), created_ids)
            .await?;
        if let Value::Object(map) = &mut response {
            map.insert("method".to_string(), Value::String(method_name.to_string()));
        }
        Ok(response)
    }

    async fn apply_reminder_mutation(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        value: &Value,
        default_set: bool,
        audit_subject: &str,
    ) -> Result<String> {
        let source_type = value
            .get("sourceType")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("sourceType is required"))?;
        let source_id = value
            .get("sourceId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("sourceId is required"))
            .and_then(parse_uuid)?;
        let reminder_set = value
            .get("reminderSet")
            .and_then(Value::as_bool)
            .unwrap_or(default_set);
        let reminder_at = value
            .get("reminderAt")
            .and_then(Value::as_str)
            .map(str::to_string);
        let dismissed_at = value
            .get("dismissedAt")
            .or_else(|| value.get("reminderDismissedAt"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let occurrence_start_at = value
            .get("occurrenceStartAt")
            .and_then(Value::as_str)
            .map(str::to_string);
        let reminder_reset = value.get("reminderReset").and_then(Value::as_bool);

        match source_type {
            "task" => {
                if let (Some(occurrence_start_at), Some(dismissed_at)) =
                    (occurrence_start_at.clone(), dismissed_at.clone())
                {
                    self.store
                        .dismiss_jmap_reminder_occurrence(
                            account_id,
                            source_type.to_string(),
                            source_id,
                            occurrence_start_at,
                            dismissed_at,
                        )
                        .await?;
                } else {
                    self.store
                        .update_jmap_task_reminder(
                            account_id,
                            source_id,
                            Some(reminder_set),
                            reminder_at,
                            dismissed_at,
                            reminder_reset,
                        )
                        .await?;
                }
            }
            "calendar" => {
                if let (Some(occurrence_start_at), Some(dismissed_at)) =
                    (occurrence_start_at, dismissed_at.clone())
                {
                    self.store
                        .dismiss_jmap_reminder_occurrence(
                            account_id,
                            source_type.to_string(),
                            source_id,
                            occurrence_start_at,
                            dismissed_at,
                        )
                        .await?;
                } else {
                    self.store
                        .update_jmap_event_reminder(
                            account_id,
                            source_id,
                            Some(reminder_set),
                            reminder_at,
                            dismissed_at,
                        )
                        .await?;
                }
            }
            "mail" => {
                self.store
                    .update_jmap_mail_reminder(
                        account_id,
                        source_id,
                        Some(reminder_set),
                        reminder_at,
                        dismissed_at,
                        AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-reminder-update".to_string(),
                            subject: audit_subject.to_string(),
                        },
                    )
                    .await?;
            }
            _ => bail!("unsupported reminder sourceType"),
        }
        Ok(format!("{source_type}:{source_id}"))
    }

    pub(crate) async fn handle_share_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let old_state = self
            .canonical_object_state(account, account_id, "Share")
            .await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.get("create").and_then(Value::as_object) {
            for (creation_id, value) in create {
                match parse_share_input(account_id, value).and_then(|input| {
                    Ok((
                        input,
                        share_audit(account, "jmap-share-upsert", creation_id),
                    ))
                }) {
                    Ok((input, audit)) => match self.store.upsert_jmap_share(input, audit).await {
                        Ok(share) => {
                            let id = share
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or(creation_id)
                                .to_string();
                            created_ids.insert(creation_id.clone(), id.clone());
                            created.insert(creation_id.clone(), share);
                        }
                        Err(error) => {
                            not_created.insert(creation_id.clone(), set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_created.insert(creation_id.clone(), set_error(&error.to_string()));
                    }
                }
            }
        }
        if let Some(update) = arguments.get("update").and_then(Value::as_object) {
            for (id, value) in update {
                let mut object = self
                    .canonical_objects(account, account_id, "Share")
                    .await?
                    .into_iter()
                    .find(|share| share.get("id").and_then(Value::as_str) == Some(id.as_str()))
                    .unwrap_or_else(|| json!({}));
                if let (Value::Object(base), Value::Object(patch)) = (&mut object, value) {
                    for (key, value) in patch {
                        base.insert(key.clone(), value.clone());
                    }
                }
                match parse_share_input(account_id, &object) {
                    Ok(input) => match self
                        .store
                        .upsert_jmap_share(input, share_audit(account, "jmap-share-upsert", id))
                        .await
                    {
                        Ok(_) => {
                            updated.insert(id.clone(), Value::Object(Map::new()));
                        }
                        Err(error) => {
                            not_updated.insert(id.clone(), set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_updated.insert(id.clone(), set_error(&error.to_string()));
                    }
                }
            }
        }
        if let Some(ids) = arguments.get("destroy").and_then(Value::as_array) {
            let shares = self.canonical_objects(account, account_id, "Share").await?;
            for value in ids {
                let Some(id) = value.as_str() else {
                    continue;
                };
                let Some(share) = shares
                    .iter()
                    .find(|share| share.get("id").and_then(Value::as_str) == Some(id))
                    .cloned()
                else {
                    not_destroyed.insert(id.to_string(), set_error("share not found"));
                    continue;
                };
                match self
                    .store
                    .delete_jmap_share(share, share_audit(account, "jmap-share-delete", id))
                    .await
                {
                    Ok(()) => destroyed.push(Value::String(id.to_string())),
                    Err(error) => {
                        not_destroyed.insert(id.to_string(), set_error(&error.to_string()));
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": self.canonical_object_state(account, account_id, "Share").await?,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    pub(crate) async fn handle_share_import_or_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
        method_name: &str,
    ) -> Result<Value> {
        let mut set_arguments = Map::new();
        if let Some(account_id) = arguments.get("accountId").cloned() {
            set_arguments.insert("accountId".to_string(), account_id);
        }
        set_arguments.insert(
            "create".to_string(),
            arguments
                .get("create")
                .cloned()
                .unwrap_or_else(|| json!({})),
        );
        let mut response = self
            .handle_share_set(account, Value::Object(set_arguments), created_ids)
            .await?;
        if let Value::Object(map) = &mut response {
            map.insert("method".to_string(), Value::String(method_name.to_string()));
        }
        Ok(response)
    }

    pub(crate) async fn handle_search_folder_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let old_state = self
            .canonical_object_state(account, account_id, "SearchFolder")
            .await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.get("create").and_then(Value::as_object) {
            for (creation_id, value) in create {
                match search_folder_input_from_value(None, account_id, value) {
                    Ok(input) => match self.store.upsert_search_folder(input).await {
                        Ok(folder) => {
                            created_ids.insert(creation_id.clone(), folder.id.to_string());
                            created
                                .insert(creation_id.clone(), json!({"id": folder.id.to_string()}));
                        }
                        Err(error) => {
                            not_created.insert(creation_id.clone(), set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_created.insert(creation_id.clone(), set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.get("update").and_then(Value::as_object) {
            for (id, value) in update {
                match parse_uuid(id).and_then(|folder_id| {
                    search_folder_input_from_value(Some(folder_id), account_id, value)
                }) {
                    Ok(input) => match self.store.upsert_search_folder(input).await {
                        Ok(_) => {
                            updated.insert(id.clone(), Value::Object(Map::new()));
                        }
                        Err(error) => {
                            not_updated.insert(id.clone(), set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_updated.insert(id.clone(), set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = string_ids_from_arguments(&arguments, "destroy") {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(folder_id) => {
                        match self.store.delete_search_folder(account_id, folder_id).await {
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

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": self.canonical_object_state(account, account_id, "SearchFolder").await?,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    pub(crate) async fn handle_search_folder_import_or_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
        method_name: &str,
    ) -> Result<Value> {
        let mut set_arguments = Map::new();
        if let Some(account_id) = arguments.get("accountId").cloned() {
            set_arguments.insert("accountId".to_string(), account_id);
        }
        set_arguments.insert(
            "create".to_string(),
            arguments
                .get("create")
                .or_else(|| arguments.get("searchFolders"))
                .cloned()
                .unwrap_or_else(|| json!({})),
        );
        let mut response = self
            .handle_search_folder_set(account, Value::Object(set_arguments), created_ids)
            .await?;
        if let Value::Object(map) = &mut response {
            map.insert("method".to_string(), Value::String(method_name.to_string()));
        }
        Ok(response)
    }

    pub(crate) async fn authenticate(
        &self,
        authorization: Option<&str>,
    ) -> Result<AuthenticatedAccount> {
        let token = bearer_token(authorization).ok_or_else(|| anyhow!("missing bearer token"))?;
        self.store
            .fetch_account_session(token)
            .await?
            .ok_or_else(|| anyhow!("invalid or expired account session"))
    }
}
