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
    JmapUploadBlob, MailboxAccountAccess, SenderIdentity, Storage,
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
                        "Calendar/get" => self.handle_calendar_get(account, arguments).await,
                        "Calendar/query" => self.handle_calendar_query(account, arguments).await,
                        "Calendar/queryChanges" => {
                            self.handle_calendar_query_changes(account, arguments).await
                        }
                        "Calendar/changes" => {
                            self.handle_calendar_changes(account, arguments).await
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
                        "Calendar/set" | "Calendar/import" | "Calendar/copy" => {
                            self.handle_canonical_unsupported_write(
                                account,
                                arguments,
                                "Calendar",
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

    pub(crate) async fn object_state(&self, account_id: Uuid, data_type: &str) -> Result<String> {
        let entries = self.object_state_entries(account_id, data_type).await?;
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?;
        encode_state_with_cursor(account_id, data_type, entries, cursor)
    }

    pub(crate) async fn object_changes_response(
        &self,
        account_id: Uuid,
        data_type: &str,
        since_state: &str,
        max_changes: Option<u64>,
        entries: Vec<StateEntry>,
    ) -> Result<Value> {
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?;
        if let Some(after_cursor) = state_cursor(account_id, data_type, since_state)? {
            if let Some(changes) = self
                .store
                .replay_jmap_object_changes(
                    account_id,
                    data_type,
                    after_cursor,
                    crate::store::MAX_JMAP_MAIL_OBJECT_REPLAY_ROWS,
                )
                .await?
            {
                return changes_response_from_durable_with_cursor(
                    account_id,
                    data_type,
                    since_state,
                    max_changes,
                    entries,
                    cursor,
                    changes
                        .into_iter()
                        .map(|change| DurableObjectChange {
                            id: change.object_id.to_string(),
                        })
                        .collect(),
                );
            }
        }
        changes_response_with_cursor(
            account_id,
            data_type,
            since_state,
            max_changes,
            entries,
            cursor,
        )
    }

    pub(crate) async fn mailbox_object_state(
        &self,
        access: &MailboxAccountAccess,
    ) -> Result<String> {
        let entries = self.mailbox_object_state_entries(access).await?;
        let cursor = self
            .store
            .fetch_jmap_mail_change_cursor(access.account_id)
            .await?;
        encode_state_with_cursor(access.account_id, "Mailbox", entries, cursor)
    }

    pub(crate) async fn mailbox_object_state_entries(
        &self,
        access: &MailboxAccountAccess,
    ) -> Result<Vec<StateEntry>> {
        let mailboxes = self.store.fetch_jmap_mailboxes(access.account_id).await?;
        Ok(mailboxes
            .into_iter()
            .map(|mailbox| StateEntry {
                id: mailbox.id.to_string(),
                fingerprint: mailbox_state_fingerprint(&mailbox, Some(access)),
            })
            .collect())
    }

    pub(crate) async fn mail_object_state(
        &self,
        access: &MailboxAccountAccess,
        data_type: &str,
    ) -> Result<String> {
        let entries = self.mail_object_state_entries(access, data_type).await?;
        let cursor = self
            .store
            .fetch_jmap_mail_change_cursor(access.account_id)
            .await?;
        encode_state_with_cursor(access.account_id, data_type, entries, cursor)
    }

    pub(crate) async fn email_delivery_object_state(&self, account_id: Uuid) -> Result<String> {
        let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let entries = emails
            .into_iter()
            .map(|email| StateEntry {
                id: email.id.to_string(),
                fingerprint: opaque_state_fingerprint(&email.received_at),
            })
            .collect();
        encode_state(account_id, "EmailDelivery", entries)
    }

    pub(crate) async fn email_submission_object_state(&self, account_id: Uuid) -> Result<String> {
        let entries = self
            .email_submission_object_state_entries(account_id)
            .await?;
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, "EmailSubmission")
            .await?;
        encode_state_with_cursor(account_id, "EmailSubmission", entries, cursor)
    }

    pub(crate) async fn email_submission_object_state_entries(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<StateEntry>> {
        let submissions = self
            .store
            .fetch_jmap_email_submissions(account_id, &[])
            .await?;
        Ok(submissions
            .into_iter()
            .map(|submission| StateEntry {
                id: submission.id.to_string(),
                fingerprint: email_submission_state_fingerprint(&submission),
            })
            .collect())
    }

    pub(crate) async fn identity_object_state(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<String> {
        let entries = self
            .identity_object_state_entries(principal_account_id, target_account_id)
            .await?;
        encode_state(target_account_id, "Identity", entries)
    }

    pub(crate) async fn identity_object_state_entries(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<Vec<StateEntry>> {
        let identities = self
            .store
            .fetch_sender_identities(principal_account_id, target_account_id)
            .await?;
        Ok(identities
            .into_iter()
            .map(|identity| StateEntry {
                id: identity.id.clone(),
                fingerprint: identity_state_fingerprint(&identity),
            })
            .collect())
    }

    pub(crate) async fn mail_object_state_entries(
        &self,
        access: &MailboxAccountAccess,
        data_type: &str,
    ) -> Result<Vec<StateEntry>> {
        self.mail_object_state_entries_with_bcc(access.account_id, data_type, access.is_owned)
            .await
    }

    async fn mail_object_state_entries_with_bcc(
        &self,
        account_id: Uuid,
        data_type: &str,
        include_bcc: bool,
    ) -> Result<Vec<StateEntry>> {
        match data_type {
            "Email" => {
                let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
                let emails = if include_bcc {
                    self.store
                        .fetch_jmap_emails_with_protected_bcc(account_id, &ids)
                        .await?
                } else {
                    self.store.fetch_jmap_emails(account_id, &ids).await?
                };
                Ok(emails
                    .into_iter()
                    .map(|email| StateEntry {
                        id: email.id.to_string(),
                        fingerprint: email_state_fingerprint(&email, include_bcc),
                    })
                    .collect())
            }
            "Thread" => {
                let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
                let emails = if include_bcc {
                    self.store
                        .fetch_jmap_emails_with_protected_bcc(account_id, &ids)
                        .await?
                } else {
                    self.store.fetch_jmap_emails(account_id, &ids).await?
                };
                let mut threads: HashMap<Uuid, Vec<String>> = HashMap::new();
                for email in emails {
                    threads.entry(email.thread_id).or_default().push(format!(
                        "{}:{}",
                        email.id,
                        email_state_fingerprint(&email, include_bcc)
                    ));
                }
                let mut entries = threads
                    .into_iter()
                    .map(|(thread_id, mut fingerprints)| {
                        fingerprints.sort();
                        StateEntry {
                            id: thread_id.to_string(),
                            fingerprint: opaque_state_fingerprint(&fingerprints.join("|")),
                        }
                    })
                    .collect::<Vec<_>>();
                entries.sort_by(|left, right| left.id.cmp(&right.id));
                Ok(entries)
            }
            _ => Ok(Vec::new()),
        }
    }

    pub(crate) async fn object_state_entries(
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
                        fingerprint: mailbox_state_fingerprint(&mailbox, None),
                    })
                    .collect())
            }
            "Email" | "Thread" => {
                self.mail_object_state_entries_with_bcc(account_id, data_type, true)
                    .await
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
            "TaskList" => {
                let task_lists = self.store.fetch_jmap_task_lists(account_id).await?;
                Ok(task_lists
                    .into_iter()
                    .map(|task_list| StateEntry {
                        id: task_list.id.to_string(),
                        fingerprint: task_list_state_fingerprint(&task_list),
                    })
                    .collect())
            }
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
            "Note" => {
                let notes = self.store.fetch_jmap_notes(account_id).await?;
                Ok(notes
                    .into_iter()
                    .map(|note| StateEntry {
                        id: note.id.to_string(),
                        fingerprint: crate::notes_journal::note_state_fingerprint(&note),
                    })
                    .collect())
            }
            "JournalEntry" => {
                let entries = self.store.fetch_jmap_journal_entries(account_id).await?;
                Ok(entries
                    .into_iter()
                    .map(|entry| StateEntry {
                        id: entry.id.to_string(),
                        fingerprint: crate::notes_journal::journal_entry_state_fingerprint(&entry),
                    })
                    .collect())
            }
            "Reminder" => {
                let reminders = self
                    .store
                    .query_jmap_reminders(
                        account_id,
                        lpe_storage::ReminderQuery {
                            include_inactive: true,
                        },
                    )
                    .await?;
                Ok(reminders
                    .into_iter()
                    .map(|reminder| StateEntry {
                        id: format!("{}:{}", reminder.source_type, reminder.source_id),
                        fingerprint: crate::notes_journal::reminder_state_fingerprint(&reminder),
                    })
                    .collect())
            }
            _ => Ok(Vec::new()),
        }
    }

    pub(crate) async fn handle_canonical_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let ids = string_ids_from_arguments(&arguments, "ids");
        let properties = property_names_from_arguments(&arguments);
        let ids_set = ids
            .as_ref()
            .map(|ids| ids.iter().cloned().collect::<HashSet<_>>());
        let list = self
            .canonical_objects(account, account_id, data_type)
            .await?
            .into_iter()
            .filter(|object| {
                ids_set
                    .as_ref()
                    .map(|ids| {
                        object
                            .get("id")
                            .and_then(Value::as_str)
                            .is_some_and(|id| ids.contains(id))
                    })
                    .unwrap_or(true)
            })
            .map(|object| project_get_properties(object, properties.as_ref()))
            .collect::<Vec<_>>();
        let not_found = ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| {
                !list
                    .iter()
                    .any(|object| object.get("id").and_then(Value::as_str) == Some(id.as_str()))
            })
            .map(Value::String)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": self.canonical_object_state(account, account_id, data_type).await?,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_canonical_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let mut all_ids = self
            .canonical_objects(account, account_id, data_type)
            .await?
            .into_iter()
            .filter_map(|object| object.get("id").and_then(Value::as_str).map(str::to_string))
            .collect::<Vec<_>>();
        all_ids.sort();
        let position = query_position(
            &all_ids,
            arguments.get("position").and_then(Value::as_i64),
            arguments.get("anchor").and_then(Value::as_str),
            arguments.get("anchorOffset").and_then(Value::as_i64),
        )?;
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = all_ids
            .iter()
            .skip(position)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let total = all_ids.len();
        let method_name = format!("{data_type}/query");
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?
            .unwrap_or(0);
        let query_state = match self
            .store
            .save_jmap_query_state(account_id, &method_name, None, None, cursor, &all_ids)
            .await?
        {
            Some(state_id) => encode_query_state_reference(
                account_id,
                &method_name,
                None,
                None,
                state_id,
                cursor,
            )?,
            None => encode_query_state(account_id, &method_name, None, None, all_ids)?,
        };

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": query_state,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": total,
        }))
    }

    pub(crate) async fn handle_canonical_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let since_query_state = arguments
            .get("sinceQueryState")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("sinceQueryState is required"))?
            .to_string();
        let mut ids = self
            .canonical_query_ids(account, account_id, data_type, &arguments)
            .await?;
        ids.sort();
        let total = ids.len() as u64;
        let method_name = canonical_query_state_method(data_type);
        let filter_state = canonical_query_filter(data_type, &arguments);
        let previous = decode_query_state(&since_query_state)?;
        validate_query_state_token(
            account_id,
            &method_name,
            filter_state.as_ref(),
            None,
            &previous,
        )?;
        let mut previous_cursor = previous.cursor.unwrap_or(0);
        let previous_ids =
            if let Some(state_id) = previous.state_id.as_deref().map(parse_uuid).transpose()? {
                let stored = self
                    .store
                    .fetch_jmap_query_state(
                        account_id,
                        &method_name,
                        state_id,
                        filter_state.clone(),
                        None,
                    )
                    .await?
                    .ok_or_else(|| anyhow!("queryState is no longer available"))?;
                previous_cursor = stored.last_change_sequence;
                stored.snapshot_ids
            } else {
                previous.ids.clone()
            };
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?
            .unwrap_or(0);
        let diff = query_diff_for_kind(
            &method_name,
            &previous_ids,
            &ids,
            arguments.get("maxChanges").and_then(Value::as_u64),
        );
        let next_cursor = if diff.has_more_changes {
            previous_cursor
        } else {
            cursor
        };
        let next_query_state = match self
            .store
            .save_jmap_query_state(
                account_id,
                &method_name,
                filter_state.clone(),
                None,
                next_cursor,
                &diff.query_state_ids,
            )
            .await?
        {
            Some(state_id) => encode_query_state_reference(
                account_id,
                &method_name,
                filter_state.clone(),
                None,
                state_id,
                next_cursor,
            )?,
            None => encode_query_state(
                account_id,
                &method_name,
                filter_state.clone(),
                None,
                diff.query_state_ids.clone(),
            )?,
        };
        query_changes_response_from_diff(
            account_id,
            &method_name,
            since_query_state,
            filter_state,
            None,
            previous,
            next_query_state,
            total,
            diff,
        )
    }

    pub(crate) async fn handle_canonical_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let since_state = arguments
            .get("sinceState")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("sinceState is required"))?;
        let max_changes = arguments.get("maxChanges").and_then(Value::as_u64);
        let entries = self
            .canonical_objects(account, account_id, data_type)
            .await?
            .into_iter()
            .filter_map(|object| {
                let id = object.get("id")?.as_str()?.to_string();
                Some(StateEntry {
                    id,
                    fingerprint: opaque_state_fingerprint(&object.to_string()),
                })
            })
            .collect::<Vec<_>>();
        if matches!(data_type, "Share" | "Reminder") {
            return self
                .string_object_changes_response(
                    account_id,
                    data_type,
                    since_state,
                    max_changes,
                    entries,
                )
                .await;
        }
        self.object_changes_response(account_id, data_type, since_state, max_changes, entries)
            .await
    }

    pub(crate) async fn string_object_changes_response(
        &self,
        account_id: Uuid,
        data_type: &str,
        since_state: &str,
        max_changes: Option<u64>,
        entries: Vec<StateEntry>,
    ) -> Result<Value> {
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?;
        if let Some(after_cursor) = state_cursor(account_id, data_type, since_state)? {
            if let Some(changes) = self
                .store
                .replay_jmap_string_object_changes(
                    account_id,
                    data_type,
                    after_cursor,
                    crate::store::MAX_JMAP_MAIL_OBJECT_REPLAY_ROWS,
                )
                .await?
            {
                return changes_response_from_durable_with_cursor(
                    account_id,
                    data_type,
                    since_state,
                    max_changes,
                    entries,
                    cursor,
                    changes
                        .into_iter()
                        .map(|change| DurableObjectChange {
                            id: change.object_id,
                        })
                        .collect(),
                );
            }
        }
        changes_response_with_cursor(
            account_id,
            data_type,
            since_state,
            max_changes,
            entries,
            cursor,
        )
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
                    let (source_type, source_id) = parse_reminder_id(id)?;
                    map.entry("sourceType")
                        .or_insert_with(|| Value::String(source_type));
                    map.entry("sourceId")
                        .or_insert_with(|| Value::String(source_id.to_string()));
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
                let (source_type, source_id) = match parse_reminder_id(id) {
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

        match source_type {
            "task" => {
                self.store
                    .update_jmap_task_reminder(
                        account_id,
                        source_id,
                        Some(reminder_set),
                        reminder_at,
                    )
                    .await?;
            }
            "calendar" => {
                self.store
                    .update_jmap_event_reminder(
                        account_id,
                        source_id,
                        Some(reminder_set),
                        reminder_at,
                    )
                    .await?;
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

    pub(crate) async fn handle_canonical_import_or_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
        data_type: &str,
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
        let mut response = match data_type {
            "ContactCard" => {
                self.handle_contact_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "CalendarEvent" => {
                self.handle_calendar_event_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "TaskList" => {
                self.handle_task_list_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "Task" => {
                self.handle_task_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "Note" => {
                self.handle_note_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "JournalEntry" => {
                self.handle_journal_entry_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            _ => {
                return self
                    .handle_canonical_unsupported_write(account, arguments, data_type, method_name)
                    .await;
            }
        };
        if let Value::Object(map) = &mut response {
            map.insert("method".to_string(), Value::String(method_name.to_string()));
        }
        Ok(response)
    }

    pub(crate) async fn handle_canonical_unsupported_write(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
        method_name: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let old_state = self
            .canonical_object_state(account, account_id, data_type)
            .await?;
        let mut not_created = Map::new();
        let mut not_updated = Map::new();
        let mut not_destroyed = Map::new();
        for id in canonical_create_ids(&arguments) {
            not_created.insert(
                id,
                json!({
                    "type": "forbidden",
                    "description": format!("{method_name} is not a canonical write surface for {data_type}"),
                }),
            );
        }
        if method_name.ends_with("/set") {
            for id in object_keys(&arguments, "update") {
                not_updated.insert(
                    id,
                    json!({
                        "type": "forbidden",
                        "description": format!("{method_name} is not a canonical write surface for {data_type}"),
                    }),
                );
            }
            for id in string_ids_from_arguments(&arguments, "destroy").unwrap_or_default() {
                not_destroyed.insert(
                    id,
                    json!({
                        "type": "forbidden",
                        "description": format!("{method_name} is not a canonical write surface for {data_type}"),
                    }),
                );
            }
        }
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": self.canonical_object_state(account, account_id, data_type).await?,
            "created": {},
            "notCreated": Value::Object(not_created),
            "updated": {},
            "notUpdated": Value::Object(not_updated),
            "destroyed": [],
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    pub(crate) async fn canonical_object_state(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<String> {
        match data_type {
            "Identity" => {
                self.identity_object_state(account.account_id, account_id)
                    .await
            }
            "EmailSubmission" => self.email_submission_object_state(account_id).await,
            "Mailbox" => {
                let access = self
                    .requested_account_access(account, Some(&account_id.to_string()))
                    .await?;
                self.mailbox_object_state(&access).await
            }
            "Email" | "Thread" => {
                let access = self
                    .requested_account_access(account, Some(&account_id.to_string()))
                    .await?;
                self.mail_object_state(&access, data_type).await
            }
            "Blob" | "DurableChange" => {
                let entries = self
                    .canonical_objects(account, account_id, data_type)
                    .await?
                    .into_iter()
                    .filter_map(|object| {
                        let id = object.get("id")?.as_str()?.to_string();
                        Some(StateEntry {
                            id,
                            fingerprint: opaque_state_fingerprint(&object.to_string()),
                        })
                    })
                    .collect();
                encode_state(account_id, data_type, entries)
            }
            "Share" | "Reminder" => {
                let entries = self
                    .canonical_objects(account, account_id, data_type)
                    .await?
                    .into_iter()
                    .filter_map(|object| {
                        let id = object.get("id")?.as_str()?.to_string();
                        Some(StateEntry {
                            id,
                            fingerprint: opaque_state_fingerprint(&object.to_string()),
                        })
                    })
                    .collect();
                let cursor = self
                    .store
                    .fetch_jmap_object_change_cursor(account_id, data_type)
                    .await?;
                encode_state_with_cursor(account_id, data_type, entries, cursor)
            }
            _ => self.object_state(account_id, data_type).await,
        }
    }

    async fn canonical_objects(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<Vec<Value>> {
        match data_type {
            "Identity" => Ok(self
                .store
                .fetch_sender_identities(account.account_id, account_id)
                .await?
                .into_iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<_>, _>>()?),
            "Reminder" => Ok(self
                .store
                .query_jmap_reminders(
                    account_id,
                    lpe_storage::ReminderQuery {
                        include_inactive: true,
                    },
                )
                .await?
                .into_iter()
                .map(|reminder| {
                    let id = format!("{}:{}", reminder.source_type, reminder.source_id);
                    let mut object = serde_json::to_value(reminder)?;
                    if let Value::Object(map) = &mut object {
                        map.insert("id".to_string(), Value::String(id));
                        map.insert("@type".to_string(), Value::String("Reminder".to_string()));
                    }
                    Ok(object)
                })
                .collect::<Result<Vec<_>>>()?),
            "Share" => self.store.fetch_jmap_shares(account_id).await,
            "DurableChange" => {
                let cursor = self.store.fetch_canonical_change_cursor(account_id).await?;
                Ok(vec![json!({
                    "id": "canonical",
                    "@type": "DurableChange",
                    "scope": "account",
                    "cursor": cursor,
                    "isAppendOnly": true,
                    "mayRead": true,
                    "mayWrite": false,
                    "categories": [
                        {"id": "mail", "objectTypes": ["Mailbox", "Email", "Thread", "EmailSubmission", "Blob"]},
                        {"id": "contacts", "objectTypes": ["AddressBook", "ContactCard"]},
                        {"id": "calendar", "objectTypes": ["Calendar", "CalendarEvent"]},
                        {"id": "tasks", "objectTypes": ["TaskList", "Task", "Reminder"]},
                        {"id": "notes", "objectTypes": ["Note"]},
                        {"id": "journal", "objectTypes": ["JournalEntry"]},
                        {"id": "rights", "objectTypes": ["Identity", "Share"]},
                        {"id": "search", "objectTypes": []},
                        {"id": "rules", "objectTypes": []}
                    ],
                })])
            }
            "Blob" => Ok(Vec::new()),
            _ => Ok(self
                .object_state_entries(account_id, data_type)
                .await?
                .into_iter()
                .map(|entry| json!({"id": entry.id}))
                .collect()),
        }
    }

    async fn canonical_query_ids(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        data_type: &str,
        arguments: &Value,
    ) -> Result<Vec<String>> {
        if data_type == "Reminder" {
            return Ok(self
                .store
                .query_jmap_reminders(
                    account_id,
                    lpe_storage::ReminderQuery {
                        include_inactive: arguments
                            .get("includeInactive")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    },
                )
                .await?
                .into_iter()
                .map(|reminder| format!("{}:{}", reminder.source_type, reminder.source_id))
                .collect());
        }
        Ok(self
            .canonical_objects(account, account_id, data_type)
            .await?
            .into_iter()
            .filter_map(|object| object.get("id").and_then(Value::as_str).map(str::to_string))
            .collect())
    }

    pub(crate) async fn handle_upload(
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
        if !requested_account.is_owned && !requested_account.may_write {
            bail!("accountId is read-only");
        }
        if body.len() as u64 > MAX_SIZE_UPLOAD {
            bail!("JMAP upload exceeds maxSizeUpload");
        }
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

    pub(crate) async fn handle_download(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        blob_id: &str,
    ) -> Result<JmapUploadBlob> {
        let account = self.authenticate(authorization).await?;
        let requested_account = self
            .requested_account_access(&account, Some(account_id))
            .await?;
        self.resolve_download_blob(&requested_account, blob_id)
            .await
    }

    pub(crate) async fn resolve_download_blob(
        &self,
        requested_account: &MailboxAccountAccess,
        blob_id: &str,
    ) -> Result<JmapUploadBlob> {
        self.resolve_download_blob_with_bcc(requested_account, blob_id, false)
            .await
    }

    pub(crate) async fn resolve_download_blob_with_bcc(
        &self,
        requested_account: &MailboxAccountAccess,
        blob_id: &str,
        include_bcc: bool,
    ) -> Result<JmapUploadBlob> {
        let requested_account_id = requested_account.account_id;
        match JmapBlobId::parse(blob_id)? {
            JmapBlobId::Upload(blob_id) => self
                .store
                .fetch_jmap_upload_blob(requested_account_id, blob_id)
                .await?
                .ok_or_else(|| anyhow!("blob not found")),
            JmapBlobId::Message(message_id) => {
                if !include_bcc {
                    if let Some(blob) = self
                        .store
                        .fetch_jmap_message_blob(requested_account_id, message_id)
                        .await?
                    {
                        return Ok(blob);
                    }
                }
                let emails = if include_bcc {
                    self.store
                        .fetch_jmap_emails_with_protected_bcc(requested_account_id, &[message_id])
                        .await?
                } else {
                    self.store
                        .fetch_jmap_emails(requested_account_id, &[message_id])
                        .await?
                };
                let email = emails
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("blob not found"))?;
                let blob_bytes = message_rfc822_bytes(&email, include_bcc);
                Ok(JmapUploadBlob {
                    id: message_id,
                    account_id: requested_account_id,
                    media_type: "message/rfc822".to_string(),
                    octet_size: blob_bytes.len() as u64,
                    blob_bytes,
                })
            }
            JmapBlobId::Opaque(_) => Err(anyhow!("blob not found")),
        }
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

pub(crate) fn api_request_exceeds_call_limit(request: &JmapApiRequest) -> bool {
    request.method_calls.len() > MAX_CALLS_IN_REQUEST as usize
}

fn requested_account_id_from_arguments(
    arguments: &Value,
    account: &AuthenticatedAccount,
) -> Result<Uuid> {
    session::requested_account_id(arguments.get("accountId").and_then(Value::as_str), account)
}

fn string_ids_from_arguments(arguments: &Value, field: &str) -> Option<Vec<String>> {
    arguments.get(field).and_then(Value::as_array).map(|ids| {
        ids.iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

fn property_names_from_arguments(arguments: &Value) -> Option<HashSet<String>> {
    arguments
        .get("properties")
        .and_then(Value::as_array)
        .map(|properties| {
            properties
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<HashSet<_>>()
        })
}

fn project_get_properties(object: Value, properties: Option<&HashSet<String>>) -> Value {
    let Some(properties) = properties else {
        return object;
    };
    let Value::Object(map) = object else {
        return object;
    };
    let mut projected = Map::new();
    if let Some(id) = map.get("id") {
        projected.insert("id".to_string(), id.clone());
    }
    for property in properties {
        if property == "id" {
            continue;
        }
        if let Some(value) = map.get(property) {
            projected.insert(property.clone(), value.clone());
        }
    }
    Value::Object(projected)
}

fn object_keys(arguments: &Value, field: &str) -> Vec<String> {
    arguments
        .get(field)
        .and_then(Value::as_object)
        .map(|objects| objects.keys().cloned().collect())
        .unwrap_or_default()
}

fn canonical_create_ids(arguments: &Value) -> Vec<String> {
    let ids = object_keys(arguments, "create");
    if ids.is_empty() {
        object_keys(arguments, "emails")
    } else {
        ids
    }
}

fn parse_reminder_id(id: &str) -> Result<(String, Uuid)> {
    let (source_type, source_id) = id
        .split_once(':')
        .ok_or_else(|| anyhow!("reminder id must be sourceType:sourceId"))?;
    Ok((source_type.to_string(), parse_uuid(source_id)?))
}

fn parse_share_input(owner_account_id: Uuid, value: &Value) -> Result<JmapShareInput> {
    let share_type = value
        .get("type")
        .or_else(|| value.get("shareType"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("share type is required"))?;
    let rights = value.get("rights").and_then(Value::as_object);
    Ok(JmapShareInput {
        owner_account_id,
        share_type: share_type.to_string(),
        grantee_email: value
            .get("granteeEmail")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("granteeEmail is required"))?
            .to_string(),
        task_list_id: value
            .get("taskListId")
            .and_then(Value::as_str)
            .map(parse_uuid)
            .transpose()?,
        sender_right: value
            .get("senderRight")
            .and_then(Value::as_str)
            .map(str::to_string),
        may_read: rights
            .and_then(|rights| rights.get("mayRead"))
            .or_else(|| value.get("mayRead"))
            .and_then(Value::as_bool)
            .unwrap_or(true),
        may_write: rights
            .and_then(|rights| rights.get("mayWrite"))
            .or_else(|| value.get("mayWrite"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        may_delete: rights
            .and_then(|rights| rights.get("mayDelete"))
            .or_else(|| value.get("mayDelete"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        may_share: rights
            .and_then(|rights| rights.get("mayShare"))
            .or_else(|| value.get("mayShare"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn share_audit(account: &AuthenticatedAccount, action: &str, subject: &str) -> AuditEntryInput {
    AuditEntryInput {
        actor: account.email.clone(),
        action: action.to_string(),
        subject: subject.to_string(),
    }
}

fn canonical_query_state_method(data_type: &str) -> String {
    match data_type {
        "Reminder" => "Reminder".to_string(),
        _ => format!("{data_type}/query"),
    }
}

fn canonical_query_filter(data_type: &str, arguments: &Value) -> Option<Value> {
    if data_type == "Reminder" {
        Some(json!({
            "includeInactive": arguments
                .get("includeInactive")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        }))
    } else {
        None
    }
}

pub(crate) fn validate_declared_capabilities(request: &JmapApiRequest) -> Result<()> {
    for capability in &request.using_capabilities {
        if !is_supported_capability(capability) {
            bail!("JMAP request declares unsupported capability: {capability}");
        }
    }
    Ok(())
}

fn is_supported_capability(capability: &str) -> bool {
    matches!(
        capability,
        JMAP_CORE_CAPABILITY
            | JMAP_MAIL_CAPABILITY
            | JMAP_SUBMISSION_CAPABILITY
            | JMAP_BLOB_CAPABILITY
            | JMAP_CONTACTS_CAPABILITY
            | JMAP_CALENDARS_CAPABILITY
            | JMAP_TASKS_CAPABILITY
            | JMAP_LPE_OUTLOOK_CAPABILITY
            | JMAP_VACATION_RESPONSE_CAPABILITY
            | JMAP_WEBSOCKET_CAPABILITY
    )
}

fn method_capability(method_name: &str) -> Option<&'static str> {
    match method_name {
        "Mailbox/get"
        | "Mailbox/query"
        | "Mailbox/queryChanges"
        | "Mailbox/changes"
        | "Mailbox/set"
        | "Mailbox/import"
        | "Mailbox/copy"
        | "Email/query"
        | "Email/queryChanges"
        | "Email/get"
        | "Email/changes"
        | "Email/set"
        | "Email/copy"
        | "Email/import"
        | "Thread/query"
        | "Thread/queryChanges"
        | "Thread/get"
        | "Thread/changes"
        | "Thread/set"
        | "Thread/import"
        | "Thread/copy"
        | "Quota/get"
        | "SearchSnippet/get" => Some(JMAP_MAIL_CAPABILITY),
        "EmailSubmission/get"
        | "EmailSubmission/changes"
        | "EmailSubmission/query"
        | "EmailSubmission/queryChanges"
        | "EmailSubmission/set"
        | "EmailSubmission/import"
        | "EmailSubmission/copy"
        | "Identity/get"
        | "Identity/query"
        | "Identity/queryChanges"
        | "Identity/changes"
        | "Identity/set"
        | "Identity/import"
        | "Identity/copy" => Some(JMAP_SUBMISSION_CAPABILITY),
        "AddressBook/get"
        | "AddressBook/query"
        | "AddressBook/queryChanges"
        | "AddressBook/changes"
        | "AddressBook/set"
        | "AddressBook/import"
        | "AddressBook/copy"
        | "ContactCard/get"
        | "ContactCard/query"
        | "ContactCard/queryChanges"
        | "ContactCard/changes"
        | "ContactCard/set"
        | "ContactCard/import"
        | "ContactCard/copy" => Some(JMAP_CONTACTS_CAPABILITY),
        "Calendar/get"
        | "Calendar/query"
        | "Calendar/queryChanges"
        | "Calendar/changes"
        | "Calendar/set"
        | "Calendar/import"
        | "Calendar/copy"
        | "CalendarEvent/get"
        | "CalendarEvent/query"
        | "CalendarEvent/queryChanges"
        | "CalendarEvent/changes"
        | "CalendarEvent/set"
        | "CalendarEvent/import"
        | "CalendarEvent/copy" => Some(JMAP_CALENDARS_CAPABILITY),
        "TaskList/get"
        | "TaskList/query"
        | "TaskList/queryChanges"
        | "TaskList/changes"
        | "TaskList/set"
        | "TaskList/import"
        | "TaskList/copy"
        | "Task/get"
        | "Task/query"
        | "Task/queryChanges"
        | "Task/changes"
        | "Task/set"
        | "Task/import"
        | "Task/copy" => Some(JMAP_TASKS_CAPABILITY),
        "Note/get"
        | "Note/query"
        | "Note/queryChanges"
        | "Note/changes"
        | "Note/set"
        | "Note/import"
        | "Note/copy"
        | "JournalEntry/get"
        | "JournalEntry/query"
        | "JournalEntry/queryChanges"
        | "JournalEntry/changes"
        | "JournalEntry/set"
        | "JournalEntry/import"
        | "JournalEntry/copy"
        | "Reminder/get"
        | "Reminder/query"
        | "Reminder/queryChanges"
        | "Reminder/changes"
        | "Reminder/set"
        | "Reminder/import"
        | "Reminder/copy"
        | "Share/get"
        | "Share/query"
        | "Share/queryChanges"
        | "Share/changes"
        | "Share/set"
        | "Share/import"
        | "Share/copy"
        | "DurableChange/get"
        | "DurableChange/query"
        | "DurableChange/queryChanges"
        | "DurableChange/changes"
        | "DurableChange/set"
        | "DurableChange/import"
        | "DurableChange/copy" => Some(JMAP_LPE_OUTLOOK_CAPABILITY),
        "Blob/upload" | "Blob/get" | "Blob/query" | "Blob/queryChanges" | "Blob/changes"
        | "Blob/set" | "Blob/import" | "Blob/lookup" => Some(JMAP_BLOB_CAPABILITY),
        "Blob/copy" => Some(JMAP_CORE_CAPABILITY),
        "VacationResponse/get" | "VacationResponse/set" => Some(JMAP_VACATION_RESPONSE_CAPABILITY),
        _ => None,
    }
}

fn is_method_error_payload(payload: &Value) -> bool {
    payload
        .as_object()
        .and_then(|object| object.get("type"))
        .and_then(Value::as_str)
        .is_some()
}

fn resolve_result_references(
    arguments: Value,
    previous_results: &HashMap<String, (String, Value)>,
) -> std::result::Result<Value, Value> {
    let Value::Object(mut object) = arguments else {
        return Ok(arguments);
    };
    let references = object
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix('#')
                .map(|property| (key.clone(), property.to_string(), value.clone()))
        })
        .collect::<Vec<_>>();

    for (reference_key, property, reference) in references {
        if object.contains_key(&property) {
            return Err(result_reference_error(&format!(
                "result reference {reference_key} conflicts with explicit {property}"
            )));
        }
        let reference = reference.as_object().ok_or_else(|| {
            result_reference_error(&format!(
                "result reference {reference_key} must be an object"
            ))
        })?;
        let result_of = reference
            .get("resultOf")
            .and_then(Value::as_str)
            .ok_or_else(|| result_reference_error("result reference is missing resultOf"))?;
        let expected_name = reference
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| result_reference_error("result reference is missing name"))?;
        let path = reference
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| result_reference_error("result reference is missing path"))?;
        let (actual_name, payload) = previous_results.get(result_of).ok_or_else(|| {
            result_reference_error(&format!(
                "result reference target {result_of} is not available"
            ))
        })?;
        if actual_name != expected_name {
            return Err(result_reference_error(&format!(
                "result reference target {result_of} is {actual_name}, not {expected_name}"
            )));
        }
        let resolved = payload.pointer(path).ok_or_else(|| {
            result_reference_error(&format!(
                "result reference path {path} is not available on {result_of}"
            ))
        })?;
        object.remove(&reference_key);
        object.insert(property, resolved.clone());
    }

    Ok(Value::Object(object))
}

fn result_reference_error(description: &str) -> Value {
    method_error("resultReference", description)
}

fn method_object_limit_error(method_name: &str, arguments: &Value) -> Option<Value> {
    let object_count = match method_name {
        "Mailbox/get"
        | "Email/get"
        | "EmailSubmission/get"
        | "Identity/get"
        | "Thread/get"
        | "Quota/get"
        | "AddressBook/get"
        | "ContactCard/get"
        | "Calendar/get"
        | "CalendarEvent/get"
        | "TaskList/get"
        | "Task/get"
        | "Note/get"
        | "JournalEntry/get"
        | "Reminder/get"
        | "Share/get"
        | "DurableChange/get"
        | "Blob/get"
        | "VacationResponse/get" => object_array_len(arguments, "ids"),
        "SearchSnippet/get" => object_array_len(arguments, "emailIds"),
        "Blob/lookup" => object_array_len(arguments, "ids"),
        "Mailbox/set"
        | "Email/set"
        | "EmailSubmission/set"
        | "ContactCard/set"
        | "AddressBook/set"
        | "Calendar/set"
        | "CalendarEvent/set"
        | "TaskList/set"
        | "Task/set"
        | "Note/set"
        | "JournalEntry/set"
        | "Reminder/set"
        | "Identity/set"
        | "Thread/set"
        | "Blob/set"
        | "Share/set"
        | "DurableChange/set"
        | "VacationResponse/set" => set_object_count(arguments),
        "Email/copy"
        | "Mailbox/copy"
        | "Thread/copy"
        | "EmailSubmission/copy"
        | "AddressBook/copy"
        | "ContactCard/copy"
        | "Calendar/copy"
        | "CalendarEvent/copy"
        | "TaskList/copy"
        | "Task/copy"
        | "Note/copy"
        | "JournalEntry/copy"
        | "Reminder/copy"
        | "Identity/copy"
        | "Share/copy"
        | "DurableChange/copy" => object_map_len(arguments, "create"),
        "Email/import"
        | "Mailbox/import"
        | "Thread/import"
        | "EmailSubmission/import"
        | "AddressBook/import"
        | "ContactCard/import"
        | "Calendar/import"
        | "CalendarEvent/import"
        | "TaskList/import"
        | "Task/import"
        | "Note/import"
        | "JournalEntry/import"
        | "Reminder/import"
        | "Identity/import"
        | "Blob/import"
        | "Share/import"
        | "DurableChange/import" => {
            object_map_len(arguments, "emails").or_else(|| object_map_len(arguments, "create"))
        }
        "Blob/upload" => object_map_len(arguments, "create"),
        "Blob/copy" => object_array_len(arguments, "blobIds"),
        _ => None,
    };

    let limit = if method_name.ends_with("/get")
        || matches!(method_name, "SearchSnippet/get" | "Blob/lookup")
    {
        MAX_OBJECTS_IN_GET
    } else {
        MAX_OBJECTS_IN_SET
    };

    object_count
        .filter(|count| *count > limit as usize)
        .map(|count| {
            method_error(
                "tooManyObjects",
                &format!("{method_name} includes {count} objects; limit is {limit}"),
            )
        })
}

fn object_array_len(arguments: &Value, field: &str) -> Option<usize> {
    arguments.get(field).and_then(Value::as_array).map(Vec::len)
}

fn object_map_len(arguments: &Value, field: &str) -> Option<usize> {
    arguments
        .get(field)
        .and_then(Value::as_object)
        .map(serde_json::Map::len)
}

fn set_object_count(arguments: &Value) -> Option<usize> {
    let count = object_map_len(arguments, "create").unwrap_or(0)
        + object_map_len(arguments, "update").unwrap_or(0)
        + object_array_len(arguments, "destroy").unwrap_or(0);
    (count > 0).then_some(count)
}

fn authorization_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn bearer_token(authorization: Option<&str>) -> Option<&str> {
    authorization
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub(crate) fn collection_state_fingerprint(collection: &CollaborationCollection) -> String {
    opaque_state_fingerprint(&format!(
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
    ))
}

fn email_submission_state_fingerprint(submission: &JmapEmailSubmission) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
        submission.email_id,
        submission.thread_id,
        submission.identity_id,
        submission.identity_email,
        submission.envelope_mail_from,
        submission.envelope_rcpt_to.join(","),
        submission.send_at,
        submission.undo_status,
        submission.delivery_status
    ))
}

fn identity_state_fingerprint(identity: &SenderIdentity) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}",
        identity.owner_account_id,
        identity.email,
        identity.display_name,
        identity.authorization_kind,
        identity.sender_address.as_deref().unwrap_or_default(),
        identity.sender_display.as_deref().unwrap_or_default()
    ))
}

fn mailbox_state_fingerprint(
    mailbox: &JmapMailbox,
    access: Option<&MailboxAccountAccess>,
) -> String {
    let is_drafts = mailbox.role == "drafts";
    let (may_read, may_write, may_draft, may_submit) = access
        .map(|access| {
            let may_write = crate::mailboxes::mailbox_account_may_write(access);
            let may_submit = crate::mailboxes::mailbox_account_may_submit(access);
            (
                access.may_read,
                may_write,
                is_drafts && may_write && may_submit,
                is_drafts && may_submit,
            )
        })
        .unwrap_or((true, true, is_drafts, false));
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        mailbox
            .parent_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        mailbox.role,
        mailbox.name,
        mailbox.sort_order,
        mailbox.total_emails,
        mailbox.unread_emails,
        mailbox.is_subscribed,
        may_read,
        may_draft,
        may_draft,
        may_write,
        may_write,
        may_submit,
    ))
}

fn contact_state_fingerprint(contact: &AccessibleContact) -> String {
    opaque_state_fingerprint(&format!(
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
    ))
}

fn event_state_fingerprint(event: &AccessibleEvent) -> String {
    opaque_state_fingerprint(&format!(
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
    ))
}

fn task_state_fingerprint(task: &ClientTask) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        task.task_list_id,
        task.title,
        task.description,
        task.status,
        task.due_at.as_deref().unwrap_or_default(),
        task.completed_at.as_deref().unwrap_or_default(),
        task.sort_order,
        task.updated_at
    ))
}

fn task_list_state_fingerprint(task_list: &ClientTaskList) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        task_list.owner_account_id,
        task_list.owner_email,
        task_list.owner_display_name,
        task_list.is_owned,
        task_list.rights.may_read,
        task_list.rights.may_write,
        task_list.rights.may_delete,
        task_list.rights.may_share,
        task_list.name,
        task_list.role.clone().unwrap_or_default(),
        task_list.sort_order,
        task_list.updated_at
    ))
}

fn email_state_fingerprint(email: &JmapEmail, include_bcc: bool) -> String {
    opaque_state_fingerprint(
        &(format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            email.thread_id,
            format_mailbox_ids(&email.mailbox_ids),
            format_mailbox_states(&email.mailbox_states),
            email.received_at,
            email.sent_at.as_deref().unwrap_or_default(),
            email.from_display.as_deref().unwrap_or_default(),
            email.from_address,
            format_addresses(&email.to),
            format_addresses(&email.cc),
            include_bcc
                .then(|| format_addresses(&email.bcc))
                .unwrap_or_default(),
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
        )),
    )
}

fn format_mailbox_ids(mailbox_ids: &[Uuid]) -> String {
    let mut values = mailbox_ids.iter().map(Uuid::to_string).collect::<Vec<_>>();
    values.sort();
    values.join(",")
}

fn format_mailbox_states(states: &[lpe_storage::JmapEmailMailboxState]) -> String {
    let mut values = states
        .iter()
        .map(|state| {
            format!(
                "{}:{}:{}:{}:{}:{}",
                state.mailbox_id, state.role, state.name, state.unread, state.flagged, state.draft
            )
        })
        .collect::<Vec<_>>();
    values.sort();
    values.join("|")
}

pub(crate) fn opaque_state_fingerprint(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

pub(crate) fn trim_snippet(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        normalized.chars().take(max_chars).collect::<String>()
    }
}
