use anyhow::{anyhow, bail, Result};
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::State,
    http::{
        header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, Method, StatusCode, Uri,
    },
    response::{IntoResponse, Response},
    routing::{any, on, MethodFilter},
    Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_core::sieve::{Action, Statement};
use lpe_magika::{
    Detector, ExpectedKind, IngressContext, PolicyDecision, SystemDetector, ValidationRequest,
    Validator,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{
    calendar_attendee_labels, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, AccessibleContact, AccessibleEvent,
    ActiveSyncAttachment, ActiveSyncAttachmentContent, AttachmentUploadInput, AuditEntryInput,
    CalendarOrganizerMetadata, CalendarParticipantMetadata, CalendarParticipantsMetadata,
    ClientTask, CollaborationCollection, JmapEmail, JmapEmailAddress, JmapImportedEmailInput,
    JmapMailbox, JmapMailboxCreateInput, Storage, SubmitMessageInput, SubmittedRecipientInput,
    UpsertClientContactInput, UpsertClientEventInput, UpsertClientTaskInput,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    mapi::{self, MapiEndpoint},
    ntlm,
    store::{
        ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry, ExchangeAddressBookEntryKind,
        ExchangeStore,
    },
};

const EWS_PATH: &str = "/EWS/Exchange.asmx";
const EWS_LOWER_PATH: &str = "/ews/exchange.asmx";
const MAPI_EMSMDB_PATH: &str = "/mapi/emsmdb";
const MAPI_EMSMDB_TRAILING_PATH: &str = "/mapi/emsmdb/";
const MAPI_NSPI_PATH: &str = "/mapi/nspi";
const MAPI_NSPI_TRAILING_PATH: &str = "/mapi/nspi/";
const RPC_PROXY_PATH: &str = "/rpc/rpcproxy.dll";
const RPC_PROXY_COMPAT_STATUS: &str = "x-lpe-rpc-proxy-status";
const RPC_PROXY_ECHO_STATUS: &str = "echo";
const RPC_PROXY_IN_CHANNEL_STATUS: &str = "in-channel-open";
const RPC_PROXY_RTS_CONNECT_STATUS: &str = "rts-connect";
const RPC_PROXY_ENDPOINT_PING_STATUS: &str = "endpoint-ping";
const RPC_PROXY_MAX_FINITE_BODY_BYTES: usize = 1024 * 1024;
const RPC_PROXY_RECEIVE_WINDOW_SIZE: u32 = 0x0001_0000;
const RPC_PROXY_OUT_CHANNEL_CONTENT_LENGTH: u32 = 0x0002_0000;
const RPC_PROXY_CONNECTION_TIMEOUT_MS: u32 = 120_000;
const RPC_PROXY_DCE_FAULT_PROTOCOL_ERROR: u32 = 0x0000_0005;
const RPC_PROXY_ECHO_BODY: [u8; 20] = [
    0x05, 0x00, 0x14, 0x03, 0x10, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x40, 0x00, 0x00, 0x00,
];
const CONTACTS_FOLDER_ID: &str = "contacts";
const CALENDAR_FOLDER_ID: &str = "calendar";
const TASKS_FOLDER_ID: &str = "tasks";
const DEFAULT_COLLECTION_ID: &str = "default";
const MAILBOX_QUERY_LIMIT: u64 = 200;

pub fn router() -> Router<Storage> {
    Router::new()
        .route(
            EWS_PATH,
            on(MethodFilter::OPTIONS, options_handler).post(post_handler),
        )
        .route(
            EWS_LOWER_PATH,
            on(MethodFilter::OPTIONS, options_handler).post(post_handler),
        )
        .route(
            MAPI_EMSMDB_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_emsmdb_post_handler),
        )
        .route(
            MAPI_EMSMDB_TRAILING_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_emsmdb_post_handler),
        )
        .route(
            MAPI_NSPI_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_nspi_post_handler),
        )
        .route(
            MAPI_NSPI_TRAILING_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_nspi_post_handler),
        )
        .route(RPC_PROXY_PATH, any(rpc_proxy_handler))
}

#[derive(Clone)]
pub(crate) struct ExchangeService<S, V = SystemDetector> {
    store: S,
    validator: Validator<V>,
}

impl<S> ExchangeService<S, SystemDetector> {
    pub(crate) fn new(store: S) -> Self {
        Self {
            store,
            validator: Validator::from_env(),
        }
    }
}

#[cfg(test)]
impl<S, V> ExchangeService<S, V> {
    pub(crate) fn new_with_validator(store: S, validator: Validator<V>) -> Self {
        Self { store, validator }
    }
}

async fn options_handler() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("OPTIONS, POST"));
    response
}

async fn post_handler(
    State(storage): State<Storage>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started_at = Instant::now();
    let operation = ews_operation_hint(&headers, body.as_ref());
    let service = ExchangeService::new(storage);
    let response = match service.handle(&headers, body.as_ref()).await {
        Ok(response) => response,
        Err(error) => {
            let response = error_response(&error);
            log_ews_connection(
                &uri,
                &headers,
                body.len(),
                operation.as_deref().unwrap_or("unknown"),
                ews_response_code(&response),
                &response,
                started_at.elapsed().as_secs_f64() * 1000.0,
                Some(error.to_string().as_str()),
                ews_response_debug_detail(&response),
            );
            return response;
        }
    };
    log_ews_connection(
        &uri,
        &headers,
        body.len(),
        operation.as_deref().unwrap_or("unknown"),
        ews_response_code(&response),
        &response,
        started_at.elapsed().as_secs_f64() * 1000.0,
        None,
        ews_response_debug_detail(&response),
    );
    response
}

async fn mapi_options_handler() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("OPTIONS, POST"));
    response.headers_mut().insert(
        "x-lpe-mapi-status",
        HeaderValue::from_static("transport-session-ready"),
    );
    response
}

async fn mapi_emsmdb_post_handler(
    State(storage): State<Storage>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    mapi_post_handler(MapiEndpoint::Emsmdb, storage, uri, headers, body).await
}

async fn mapi_nspi_post_handler(
    State(storage): State<Storage>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    mapi_post_handler(MapiEndpoint::Nspi, storage, uri, headers, body).await
}

async fn mapi_post_handler(
    endpoint: MapiEndpoint,
    storage: Storage,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started_at = Instant::now();
    let service = ExchangeService::new(storage);
    let response = match service.handle_mapi(endpoint, &headers, body.as_ref()).await {
        Ok(response) => response,
        Err(error) => {
            let response = mapi::mapi_error_response(&error);
            log_mapi_transport_connection(
                endpoint,
                &uri,
                &headers,
                body.as_ref(),
                &response,
                started_at.elapsed().as_secs_f64() * 1000.0,
                Some(error.to_string().as_str()),
            );
            return response;
        }
    };
    log_mapi_transport_connection(
        endpoint,
        &uri,
        &headers,
        body.as_ref(),
        &response,
        started_at.elapsed().as_secs_f64() * 1000.0,
        None,
    );
    response
}

async fn rpc_proxy_handler(
    State(storage): State<Storage>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let started_at = Instant::now();
    let service = ExchangeService::new(storage);

    if is_rpc_proxy_in_data_channel_request(&method, &uri, &headers) {
        let response = service
            .handle_rpc_proxy_in_data_channel(&method, &uri, &headers, body)
            .await;
        log_rpc_proxy_connection(
            &method,
            &uri,
            &headers,
            b"",
            &response,
            started_at.elapsed().as_secs_f64() * 1000.0,
        );
        return response;
    }

    let body = match to_bytes(body, RPC_PROXY_MAX_FINITE_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => {
            let response = (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("LPE RPC proxy request body rejected: {error}\n"),
            )
                .into_response();
            log_rpc_proxy_connection(
                &method,
                &uri,
                &headers,
                b"",
                &response,
                started_at.elapsed().as_secs_f64() * 1000.0,
            );
            return response;
        }
    };
    let response = service
        .handle_rpc_proxy(&method, &uri, &headers, body.as_ref())
        .await;
    log_rpc_proxy_connection(
        &method,
        &uri,
        &headers,
        body.as_ref(),
        &response,
        started_at.elapsed().as_secs_f64() * 1000.0,
    );
    response
}

fn ews_operation_hint(headers: &HeaderMap, body: &[u8]) -> Option<String> {
    decode_ews_body(headers, body)
        .ok()
        .and_then(|decoded| operation_name(&decoded))
}

fn log_ews_connection(
    uri: &Uri,
    headers: &HeaderMap,
    request_body_bytes: usize,
    operation: &str,
    ews_response_code: Option<&str>,
    response: &Response,
    duration_ms: f64,
    error: Option<&str>,
    debug_detail: Option<&str>,
) {
    let status = response.status().as_u16();
    let trace_id = mapi::safe_header(headers, "x-trace-id").unwrap_or_default();
    let user_agent = mapi::safe_header(headers, "user-agent").unwrap_or_default();
    let client_request_id = mapi::safe_header(headers, "client-request-id").unwrap_or_default();
    let x_request_id = mapi::safe_header(headers, "x-requestid").unwrap_or_default();
    let client_application = mapi::safe_header(headers, "x-clientapplication").unwrap_or_default();
    let message = "rca debug ews connection";

    if status < 400 {
        info!(
            rca_debug = true,
            adapter = "ews",
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            operation = %operation,
            ews_response_code = %ews_response_code.unwrap_or_default(),
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            client_application = %client_application,
            http_status = status,
            request_body_bytes,
            ews_debug_detail = %debug_detail.unwrap_or_default(),
            duration_ms,
            user_agent = %user_agent,
            "{message}"
        );
    } else {
        warn!(
            rca_debug = true,
            adapter = "ews",
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            operation = %operation,
            ews_response_code = %ews_response_code.unwrap_or_default(),
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            client_application = %client_application,
            http_status = status,
            request_body_bytes,
            ews_debug_detail = %debug_detail.unwrap_or_default(),
            duration_ms,
            user_agent = %user_agent,
            error = %error.unwrap_or_default(),
            "{message}"
        );
    }
}

#[derive(Clone, Debug)]
struct EwsResponseDebug {
    response_code: String,
    detail: String,
}

fn ews_response_code(response: &Response) -> Option<&str> {
    response
        .extensions()
        .get::<EwsResponseDebug>()
        .map(|debug| debug.response_code.as_str())
}

fn ews_response_debug_detail(response: &Response) -> Option<&str> {
    response
        .extensions()
        .get::<EwsResponseDebug>()
        .map(|debug| debug.detail.as_str())
        .filter(|detail| !detail.is_empty())
}

fn ews_payload_debug_detail(operation: &str, payload: &str) -> String {
    match operation {
        "CreateItem" => {
            let item_id = attribute_values_for_tag(payload, "ItemId", "Id")
                .into_iter()
                .next()
                .unwrap_or_default();
            let parent_folder_id = attribute_values_for_tag(payload, "ParentFolderId", "Id")
                .into_iter()
                .next()
                .unwrap_or_default();
            if item_id.is_empty() && parent_folder_id.is_empty() {
                String::new()
            } else {
                format!("created_item_id={item_id};parent_folder_id={parent_folder_id}")
            }
        }
        "SyncFolderItems" => {
            let sync_state = element_text(payload, "SyncState").unwrap_or_default();
            let creates = count_tag_occurrences(payload, "<t:Create>");
            let updates = count_tag_occurrences(payload, "<t:Update>");
            let deletes = count_tag_occurrences(payload, "<t:Delete>");
            format!("sync_state={sync_state};creates={creates};updates={updates};deletes={deletes}")
        }
        _ => String::new(),
    }
}

fn log_mapi_transport_connection(
    endpoint: MapiEndpoint,
    uri: &Uri,
    headers: &HeaderMap,
    request_body: &[u8],
    response: &Response,
    duration_ms: f64,
    error: Option<&str>,
) {
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let status = response.status().as_u16();
    let mapi_response_code = response_header(response, "x-responsecode").unwrap_or_default();
    let mapi_request_id = response_header(response, "x-requestid")
        .or_else(|| mapi::safe_header(headers, "x-requestid"))
        .unwrap_or_default();
    let request_type = response_header(response, "x-requesttype")
        .or_else(|| mapi::safe_header(headers, "x-requesttype"))
        .unwrap_or_default();
    let mailbox_id = query_parameter(uri.query().unwrap_or_default(), "mailboxId");
    let client_request_id = mapi::safe_header(headers, "client-request-id").unwrap_or_default();
    let client_info = mapi::safe_header(headers, "x-clientinfo").unwrap_or_default();
    let client_application = mapi::safe_header(headers, "x-clientapplication").unwrap_or_default();
    let trace_id = mapi::safe_header(headers, "x-trace-id").unwrap_or_default();
    let user_agent = mapi::safe_header(headers, "user-agent").unwrap_or_default();
    let response_payload_bytes = mapi::mapi_response_payload_bytes(response).unwrap_or(0);
    let request_body_bytes = request_body.len();
    let request_body_preview_hex = mapi::debug_payload_preview_hex(request_body);
    let response_payload_preview_hex =
        mapi::mapi_response_payload_preview_hex(response).unwrap_or_default();
    let message = "rca debug mapi transport connection";

    if status < 400 && mapi_response_code == "0" {
        info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            mailbox_id = %mailbox_id.unwrap_or_default(),
            request_type = %request_type,
            mapi_request_id = %mapi_request_id,
            client_request_id = %client_request_id,
            client_info = %client_info,
            client_application = %client_application,
            trace_id = %trace_id,
            http_status = status,
            mapi_response_code = %mapi_response_code,
            request_body_bytes,
            response_payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            duration_ms,
            user_agent = %user_agent,
            "{message}"
        );
    } else {
        warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            mailbox_id = %mailbox_id.unwrap_or_default(),
            request_type = %request_type,
            mapi_request_id = %mapi_request_id,
            client_request_id = %client_request_id,
            client_info = %client_info,
            client_application = %client_application,
            trace_id = %trace_id,
            http_status = status,
            mapi_response_code = %mapi_response_code,
            request_body_bytes,
            response_payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            duration_ms,
            user_agent = %user_agent,
            error = %error.unwrap_or_default(),
            "{message}"
        );
    }
}

fn log_rpc_proxy_connection(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    request_body: &[u8],
    response: &Response,
    duration_ms: f64,
) {
    let status = response.status().as_u16();
    let trace_id = mapi::safe_header(headers, "x-trace-id").unwrap_or_default();
    let user_agent = mapi::safe_header(headers, "user-agent").unwrap_or_default();
    let client_request_id = mapi::safe_header(headers, "client-request-id").unwrap_or_default();
    let x_request_id = mapi::safe_header(headers, "x-requestid").unwrap_or_default();
    let response_kind = response_header(response, RPC_PROXY_COMPAT_STATUS)
        .unwrap_or_else(|| "auth-challenge".into());
    let response_payload_bytes = rpc_proxy_response_payload_bytes(response).unwrap_or(0);
    let request_body_preview_hex = mapi::debug_payload_preview_hex(request_body);
    let response_payload_preview_hex =
        rpc_proxy_response_payload_preview_hex(response).unwrap_or_default();
    let message = "rca debug rpc proxy connection";

    if status < 400 {
        info!(
            rca_debug = true,
            adapter = "rpcproxy",
            method = %method,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            response_kind = %response_kind,
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            http_status = status,
            request_body_bytes = request_body.len(),
            response_payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            duration_ms,
            user_agent = %user_agent,
            message
        );
    } else {
        warn!(
            rca_debug = true,
            adapter = "rpcproxy",
            method = %method,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            response_kind = %response_kind,
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            http_status = status,
            request_body_bytes = request_body.len(),
            response_payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            duration_ms,
            user_agent = %user_agent,
            message
        );
    }
}

fn response_header(response: &Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn query_parameter(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        key.eq_ignore_ascii_case(name)
            .then(|| value.chars().take(240).collect())
    })
}

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle(&self, headers: &HeaderMap, body: &[u8]) -> Result<Response> {
        let principal = authenticate_account(&self.store, None, headers, "ews").await?;
        let body = decode_ews_body(headers, body)?;
        let operation = operation_name(&body).ok_or_else(|| anyhow!("unsupported EWS request"))?;

        let payload = match operation.as_str() {
            "SyncFolderHierarchy" => self.sync_folder_hierarchy(&principal).await?,
            "FindFolder" => self.find_folder(&principal).await?,
            "GetFolder" => self.get_folder(&principal, &body).await?,
            "FindItem" => self.find_item(&principal, &body).await?,
            "GetItem" => self.get_item(&principal, &body).await?,
            "SyncFolderItems" => self
                .sync_folder_items(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "SyncFolderItems",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "GetServerTimeZones" => get_server_time_zones_response(),
            "ResolveNames" => self.resolve_names(&principal, &body).await?,
            "GetUserAvailability" => self.get_user_availability(&principal, &body).await?,
            "CreateItem" => self.create_item(&principal, &body).await?,
            "UpdateItem" => self.update_item(&principal, &body).await?,
            "DeleteItem" => self.delete_item(&principal, &body).await?,
            "MoveItem" => self.move_item(&principal, &body).await?,
            "CopyItem" => self.copy_item(&principal, &body).await?,
            "CreateFolder" => self.create_folder(&principal, &body).await?,
            "DeleteFolder" => self.delete_folder(&principal, &body).await?,
            "GetAttachment" => self.get_attachment(&principal, &body).await?,
            "CreateAttachment" => self.create_attachment(&principal, &body).await?,
            "DeleteAttachment" => self.delete_attachment(&principal, &body).await?,
            "GetUserOofSettings" => self.get_user_oof_settings(&principal).await?,
            "SetUserOofSettings" => self.set_user_oof_settings(&principal, &body).await?,
            "GetRoomLists" => unsupported_operation_response("GetRoomLists"),
            "FindPeople" => unsupported_operation_response("FindPeople"),
            "ExpandDL" => unsupported_operation_response("ExpandDL"),
            "Subscribe" => unsupported_operation_response("Subscribe"),
            "GetDelegate" => unsupported_operation_response("GetDelegate"),
            "GetUserConfiguration" => unsupported_operation_response("GetUserConfiguration"),
            "GetSharingMetadata" => unsupported_operation_response("GetSharingMetadata"),
            "GetSharingFolder" => unsupported_operation_response("GetSharingFolder"),
            "Unsubscribe" => unsupported_operation_response("Unsubscribe"),
            "GetEvents" => unsupported_operation_response("GetEvents"),
            _ => unsupported_operation_response(&operation),
        };

        let response_code = element_text(&payload, "ResponseCode").unwrap_or_default();
        let detail = ews_payload_debug_detail(&operation, &payload);
        let mut response = soap_response(payload);
        response.extensions_mut().insert(EwsResponseDebug {
            response_code,
            detail,
        });
        Ok(response)
    }

    async fn resolve_names(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let entries = self
            .store
            .fetch_address_book_entries(principal.account_id)
            .await?;
        Ok(resolve_names_response(principal, request, &entries))
    }

    pub(crate) async fn handle_mapi(
        &self,
        endpoint: MapiEndpoint,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        mapi::handle_mapi(&self.store, &self.validator, endpoint, headers, body).await
    }

    pub(crate) async fn handle_rpc_proxy(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        request_body: &[u8],
    ) -> Response {
        match authenticate_account(&self.store, None, headers, "mapi").await {
            Ok(principal) => {
                if let Some(connect) =
                    parse_rpc_proxy_out_data_connect_request(method, headers, request_body)
                {
                    if is_rpc_proxy_endpoint_ping(uri) {
                        rpc_proxy_mailstore_ping_response(uri, connect.receive_window_size)
                    } else {
                        rpc_proxy_rts_connect_response(connect.receive_window_size)
                    }
                } else if is_rpc_proxy_echo_request(method, headers) {
                    rpc_proxy_echo_response()
                } else {
                    rpc_proxy_accepted_response(&principal)
                }
            }
            Err(error) => rpc_proxy_auth_challenge_response(&error.to_string()),
        }
    }

    pub(crate) async fn handle_rpc_proxy_in_data_channel(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: Body,
    ) -> Response {
        match authenticate_account(&self.store, None, headers, "mapi").await {
            Ok(principal) => {
                spawn_rpc_proxy_in_data_drain(
                    self.store.clone(),
                    self.validator.clone(),
                    principal,
                    method,
                    uri,
                    headers,
                    body,
                );
                rpc_proxy_in_channel_response(uri)
            }
            Err(error) => rpc_proxy_auth_challenge_response(&error.to_string()),
        }
    }

    async fn find_folder(&self, principal: &AccountPrincipal) -> Result<String> {
        let mut folders = String::new();
        for collection in self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts"));
        }
        for collection in self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar"));
        }
        for collection in self
            .store
            .fetch_accessible_task_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, TASKS_FOLDER_ID, "Task"));
        }
        for mailbox in self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?
        {
            folders.push_str(&mailbox_folder_xml(&mailbox));
        }

        Ok(format!(
            concat!(
                "<m:FindFolderResponse>",
                "<m:ResponseMessages>",
                "<m:FindFolderResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:RootFolder TotalItemsInView=\"{count}\" IncludesLastItemInRange=\"true\">",
                "<t:Folders>{folders}</t:Folders>",
                "</m:RootFolder>",
                "</m:FindFolderResponseMessage>",
                "</m:ResponseMessages>",
                "</m:FindFolderResponse>"
            ),
            folders = folders,
            count = count_folder_elements(&folders),
        ))
    }

    async fn sync_folder_hierarchy(&self, principal: &AccountPrincipal) -> Result<String> {
        let mut changes = String::new();
        let mut count = 0;
        for collection in self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for collection in self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for collection in self
            .store
            .fetch_accessible_task_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, TASKS_FOLDER_ID, "Task"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for mailbox in self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&mailbox_folder_xml(&mailbox));
            changes.push_str("</t:Create>");
            count += 1;
        }
        let sync_state = format!("folder-hierarchy:{count}");

        Ok(format!(
            concat!(
                "<m:SyncFolderHierarchyResponse>",
                "<m:ResponseMessages>",
                "<m:SyncFolderHierarchyResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:SyncState>{sync_state}</m:SyncState>",
                "<m:IncludesLastFolderInRange>true</m:IncludesLastFolderInRange>",
                "<m:Changes>{changes}</m:Changes>",
                "</m:SyncFolderHierarchyResponseMessage>",
                "</m:ResponseMessages>",
                "</m:SyncFolderHierarchyResponse>"
            ),
            sync_state = escape_xml(&sync_state),
            changes = changes,
        ))
    }

    async fn get_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let mailbox_ids = self
            .requested_mailbox_folder_ids(principal, request)
            .await?;
        if !mailbox_ids.is_empty() {
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            let mut folders = String::new();
            for mailbox_id in &mailbox_ids {
                let Some(mailbox) = mailboxes.iter().find(|mailbox| mailbox.id == *mailbox_id)
                else {
                    return Ok(get_folder_error_response(
                        "ErrorFolderNotFound",
                        "requested mailbox folder is not exposed by EWS",
                    ));
                };
                folders.push_str(&mailbox_folder_xml(mailbox));
            }

            return Ok(format!(
                concat!(
                    "<m:GetFolderResponse>",
                    "<m:ResponseMessages>",
                    "<m:GetFolderResponseMessage ResponseClass=\"Success\">",
                    "<m:ResponseCode>NoError</m:ResponseCode>",
                    "<m:Folders>{folders}</m:Folders>",
                    "</m:GetFolderResponseMessage>",
                    "</m:ResponseMessages>",
                    "</m:GetFolderResponse>"
                ),
                folders = folders,
            ));
        }

        let requested = requested_folder_kinds(request);
        if requested.is_empty() && request_contains_folder_reference(request) {
            return Ok(get_folder_error_response(
                "ErrorFolderNotFound",
                "folder not found",
            ));
        }

        let mut folders = String::new();
        for kind in requested {
            match kind {
                FolderKind::Root => {
                    folders.push_str(&root_folder_xml(
                        self.root_child_folder_count(principal).await?,
                    ));
                }
                FolderKind::Contacts => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_contact_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| {
                                folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts")
                            })
                            .collect::<String>(),
                    );
                }
                FolderKind::Calendar => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_calendar_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| {
                                folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar")
                            })
                            .collect::<String>(),
                    );
                }
                FolderKind::Tasks => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_task_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| folder_xml(&collection, TASKS_FOLDER_ID, "Task"))
                            .collect::<String>(),
                    );
                }
                FolderKind::Mailbox => {
                    let mailbox_ids = self
                        .requested_mailbox_folder_ids(principal, request)
                        .await?;
                    let mailboxes = self
                        .store
                        .fetch_jmap_mailboxes(principal.account_id)
                        .await?;
                    for mailbox in mailboxes.into_iter().filter(|mailbox| {
                        mailbox_ids.is_empty() || mailbox_ids.contains(&mailbox.id)
                    }) {
                        folders.push_str(&mailbox_folder_xml(&mailbox));
                    }
                }
            }
        }

        if folders.is_empty() {
            return Ok(get_folder_error_response(
                "ErrorFolderNotFound",
                "folder not found",
            ));
        }

        Ok(format!(
            concat!(
                "<m:GetFolderResponse>",
                "<m:ResponseMessages>",
                "<m:GetFolderResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:Folders>{folders}</m:Folders>",
                "</m:GetFolderResponseMessage>",
                "</m:ResponseMessages>",
                "</m:GetFolderResponse>"
            ),
            folders = folders,
        ))
    }

    async fn find_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        match requested_folder_kind(request).unwrap_or(FolderKind::Contacts) {
            FolderKind::Root => Ok(find_item_response(String::new())),
            FolderKind::Contacts => {
                let collection_id = requested_collection_id(request).unwrap_or(CONTACTS_FOLDER_ID);
                let contacts = self
                    .store
                    .fetch_accessible_contacts_in_collection(principal.account_id, collection_id)
                    .await?;
                Ok(find_item_response(
                    contacts.iter().map(contact_summary_xml).collect(),
                ))
            }
            FolderKind::Calendar => {
                let collection_id = requested_collection_id(request).unwrap_or(CALENDAR_FOLDER_ID);
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, collection_id)
                    .await?;
                Ok(find_item_response(
                    events.iter().map(calendar_item_summary_xml).collect(),
                ))
            }
            FolderKind::Tasks => {
                let collection_id = requested_collection_id(request).unwrap_or(TASKS_FOLDER_ID);
                let tasks = self
                    .store
                    .fetch_accessible_tasks_in_collection(principal.account_id, collection_id)
                    .await?;
                Ok(find_item_response(
                    tasks.iter().map(task_item_summary_xml).collect(),
                ))
            }
            FolderKind::Mailbox => {
                let Some(mailbox_id) = self
                    .requested_mailbox_folder_ids(principal, request)
                    .await?
                    .into_iter()
                    .next()
                else {
                    return Ok(find_item_response(String::new()));
                };
                let query = self
                    .store
                    .query_jmap_email_ids(
                        principal.account_id,
                        Some(mailbox_id),
                        None,
                        0,
                        MAILBOX_QUERY_LIMIT,
                    )
                    .await?;
                let emails = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &query.ids)
                    .await?;
                Ok(find_item_response(
                    emails
                        .iter()
                        .filter(|email| email.mailbox_id == mailbox_id)
                        .map(message_summary_xml)
                        .collect(),
                ))
            }
        }
    }

    async fn get_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let include_mime_content = requested_mime_content(request);
        let ids = requested_item_ids(request);
        let contact_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("contact:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let event_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("event:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let task_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("task:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let message_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("message:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let supported_id_count =
            contact_ids.len() + event_ids.len() + task_ids.len() + message_ids.len();

        let mut items = String::new();
        for contact in self
            .store
            .fetch_accessible_contacts_by_ids(principal.account_id, &contact_ids)
            .await?
        {
            items.push_str(&contact_item_xml(&contact));
        }
        for event in self
            .store
            .fetch_accessible_events_by_ids(principal.account_id, &event_ids)
            .await?
        {
            items.push_str(&calendar_item_xml(&event));
        }
        for task in self
            .store
            .fetch_accessible_tasks_by_ids(principal.account_id, &task_ids)
            .await?
        {
            items.push_str(&task_item_xml(&task));
        }
        for email in self
            .store
            .fetch_jmap_emails(principal.account_id, &message_ids)
            .await?
            .into_iter()
        {
            let attachments = if email.has_attachments {
                self.store
                    .fetch_message_attachments(principal.account_id, email.id)
                    .await?
            } else {
                Vec::new()
            };
            let mut attachment_contents = Vec::new();
            if include_mime_content {
                for attachment in &attachments {
                    let Some(content) = self
                        .store
                        .fetch_attachment_content(principal.account_id, &attachment.file_reference)
                        .await?
                    else {
                        return Ok(get_item_error_response(
                            "ErrorItemNotFound",
                            "The requested item attachment content was not found.",
                        ));
                    };
                    attachment_contents.push(content);
                }
            }
            items.push_str(&message_item_xml_with_details(
                &email,
                &attachments,
                include_mime_content.then_some(attachment_contents.as_slice()),
            ));
        }

        if !ids.is_empty()
            && (supported_id_count != ids.len()
                || count_tag_occurrences(&items, "<t:ItemId") != supported_id_count)
        {
            return Ok(get_item_error_response(
                "ErrorItemNotFound",
                "The requested item was not found or is not exposed by the EWS MVP.",
            ));
        }

        Ok(format!(
            concat!(
                "<m:GetItemResponse>",
                "<m:ResponseMessages>",
                "<m:GetItemResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:Items>{items}</m:Items>",
                "</m:GetItemResponseMessage>",
                "</m:ResponseMessages>",
                "</m:GetItemResponse>"
            ),
            items = items,
        ))
    }

    async fn get_attachment(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let ids = requested_attachment_ids(request);
        if ids.is_empty() {
            return Ok(operation_error_response(
                "GetAttachment",
                "ErrorInvalidOperation",
                "GetAttachment requires at least one AttachmentId.",
            ));
        }

        let mut attachments = String::new();
        for id in ids {
            let Some(content) = self
                .store
                .fetch_attachment_content(principal.account_id, &id)
                .await?
            else {
                return Ok(operation_error_response(
                    "GetAttachment",
                    "ErrorAttachmentNotFound",
                    "The requested attachment was not found or is not exposed by EWS.",
                ));
            };
            attachments.push_str(&file_attachment_content_xml(&content));
        }

        Ok(get_attachment_success_response(attachments))
    }

    async fn create_attachment(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let ids = requested_item_ids(request);
        let message_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("message:"))
            .map(Uuid::parse_str)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if ids.len() != 1 || message_ids.len() != 1 {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                "CreateAttachment currently supports exactly one canonical message parent id.",
            ));
        }
        if element_content(request, "ItemAttachment").is_some() {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                "CreateAttachment currently supports only FileAttachment payloads.",
            ));
        }

        let file_attachments = element_contents(request, "FileAttachment");
        if file_attachments.is_empty() {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                "CreateAttachment requires at least one FileAttachment.",
            ));
        }

        let message_id = message_ids[0];
        let mut attachments = String::new();
        let mut root_item = String::new();
        for file_attachment in file_attachments {
            let mut attachment = match parse_file_attachment_upload(file_attachment) {
                Ok(attachment) => attachment,
                Err(error) => {
                    return Ok(operation_error_response(
                        "CreateAttachment",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    ));
                }
            };

            let declared_mime = element_text(file_attachment, "ContentType")
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let outcome = self.validator.validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::ExchangeAttachment,
                    declared_mime: declared_mime.clone(),
                    filename: Some(attachment.file_name.clone()),
                    expected_kind: expected_attachment_kind(
                        &attachment.media_type,
                        &attachment.file_name,
                    ),
                },
                &attachment.blob_bytes,
            )?;
            if outcome.policy_decision != PolicyDecision::Accept {
                return Ok(operation_error_response(
                    "CreateAttachment",
                    "ErrorInvalidOperation",
                    &outcome.reason,
                ));
            }
            if declared_mime.is_none() && !outcome.detected_mime.trim().is_empty() {
                attachment.media_type = outcome.detected_mime.clone();
            }

            let Some((email, stored_attachment)) = self
                .store
                .add_message_attachment(
                    principal.account_id,
                    message_id,
                    attachment,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-create-attachment".to_string(),
                        subject: format!("message:{message_id}"),
                    },
                )
                .await?
            else {
                return Ok(operation_error_response(
                    "CreateAttachment",
                    "ErrorItemNotFound",
                    "The requested parent message was not found or is not exposed by EWS.",
                ));
            };
            root_item = root_item_id_xml(&email);
            attachments.push_str(&file_attachment_reference_xml(&stored_attachment));
        }

        Ok(create_attachment_success_response(attachments, root_item))
    }

    async fn delete_attachment(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let ids = requested_attachment_ids(request);
        if ids.is_empty() {
            return Ok(operation_error_response(
                "DeleteAttachment",
                "ErrorInvalidOperation",
                "DeleteAttachment requires at least one AttachmentId.",
            ));
        }

        let mut root_items = String::new();
        for id in ids {
            let Some(email) = self
                .store
                .delete_message_attachment(
                    principal.account_id,
                    &id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-delete-attachment".to_string(),
                        subject: id.clone(),
                    },
                )
                .await?
            else {
                return Ok(operation_error_response(
                    "DeleteAttachment",
                    "ErrorAttachmentNotFound",
                    "The requested attachment was not found or is not exposed by EWS.",
                ));
            };
            root_items.push_str(&root_item_id_xml(&email));
        }

        Ok(delete_attachment_success_response(root_items))
    }

    async fn get_user_oof_settings(&self, principal: &AccountPrincipal) -> Result<String> {
        let script = self
            .store
            .fetch_active_sieve_script(principal.account_id)
            .await?;
        Ok(get_user_oof_settings_response(&oof_projection_from_script(
            script.as_ref().map(|script| script.content.as_str()),
        )))
    }

    async fn set_user_oof_settings(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let settings = element_content(request, "OofSettings").unwrap_or(request);
            let state =
                element_text(settings, "OofState").unwrap_or_else(|| "Disabled".to_string());
            match state.trim().to_ascii_lowercase().as_str() {
                "disabled" => {
                    self.store
                        .set_active_sieve_script(
                            principal.account_id,
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-oof-disable".to_string(),
                                subject: principal.account_id.to_string(),
                            },
                        )
                        .await?;
                }
                "enabled" => {
                    let message = element_content(settings, "InternalReply")
                        .and_then(|reply| element_text(reply, "Message"))
                        .or_else(|| {
                            element_content(settings, "ExternalReply")
                                .and_then(|reply| element_text(reply, "Message"))
                        })
                        .unwrap_or_default();
                    if message.trim().is_empty() {
                        bail!("OOF message is required when enabling OOF");
                    }
                    self.store
                        .put_sieve_script(
                            principal.account_id,
                            "ews-oof",
                            &vacation_sieve_script(&message),
                            true,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-oof-enable".to_string(),
                                subject: principal.account_id.to_string(),
                            },
                        )
                        .await?;
                }
                "scheduled" => bail!("scheduled OOF is not supported by the canonical Sieve MVP"),
                other => bail!("unsupported OofState {other}"),
            }
            Ok(set_user_oof_settings_success_response())
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "SetUserOofSettings",
                "ErrorInvalidOperation",
                &error.to_string(),
            )
        }))
    }

    async fn get_user_availability(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        if let Some(mailbox) = element_content(request, "MailboxData")
            .and_then(|mailbox_data| element_content(mailbox_data, "Email"))
            .and_then(parse_mailbox)
        {
            if !mailbox.address.eq_ignore_ascii_case(&principal.email) {
                return Ok(get_user_availability_error_response(
                    "Free/busy is currently available only for the authenticated mailbox.",
                ));
            }
        }

        let (window_start, window_end) = requested_availability_window(request);
        let mut events = self
            .store
            .fetch_accessible_events_in_collection(principal.account_id, DEFAULT_COLLECTION_ID)
            .await?;
        events.retain(|event| {
            event_overlaps_window(event, window_start.as_deref(), window_end.as_deref())
        });
        events.sort_by(|left, right| {
            ews_datetime(&left.date, &left.time).cmp(&ews_datetime(&right.date, &right.time))
        });
        Ok(get_user_availability_success_response(&events))
    }

    async fn root_child_folder_count(&self, principal: &AccountPrincipal) -> Result<usize> {
        Ok(self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
            .len()
            + self
                .store
                .fetch_accessible_calendar_collections(principal.account_id)
                .await?
                .len()
            + self
                .store
                .fetch_accessible_task_collections(principal.account_id)
                .await?
                .len()
            + self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?
                .len())
    }

    async fn sync_folder_items(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let mut changes = String::new();
        let sync_state = match requested_folder_kind(request).unwrap_or(FolderKind::Contacts) {
            FolderKind::Root => "root:0".to_string(),
            FolderKind::Contacts => {
                let collection_id =
                    requested_sync_collection_id(request, "contacts", CONTACTS_FOLDER_ID);
                let contacts = self
                    .store
                    .fetch_accessible_contacts_in_collection(principal.account_id, &collection_id)
                    .await?;
                let sync_versions = sync_version_by_id(
                    self.store
                        .fetch_contact_sync_versions(principal.account_id, &collection_id)
                        .await?,
                );
                let current_items = contacts
                    .iter()
                    .map(|contact| {
                        (
                            contact.id,
                            contact_change_key(
                                contact,
                                sync_versions.get(&contact.id).map(String::as_str),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "contacts", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for contact in &contacts {
                    let current_change_key = contact_change_key(
                        contact,
                        sync_versions.get(&contact.id).map(String::as_str),
                    );
                    match previous_by_id.get(&contact.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&contact_item_xml_with_change_key(
                                contact,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&contact_item_xml_with_change_key(
                                contact,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&contact_item_xml_with_change_key(
                                contact,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let contact_id = item.id;
                    if !current_set.contains(&contact_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"contact:{contact_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("contacts", &collection_id, &current_items)
            }
            FolderKind::Calendar => {
                let collection_id =
                    requested_sync_collection_id(request, "calendar", CALENDAR_FOLDER_ID);
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, &collection_id)
                    .await?;
                let sync_versions = sync_version_by_id(
                    self.store
                        .fetch_event_sync_versions(principal.account_id, &collection_id)
                        .await?,
                );
                let current_items = events
                    .iter()
                    .map(|event| {
                        (
                            event.id,
                            calendar_change_key(
                                event,
                                sync_versions.get(&event.id).map(String::as_str),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "calendar", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for event in &events {
                    let current_change_key = calendar_change_key(
                        event,
                        sync_versions.get(&event.id).map(String::as_str),
                    );
                    match previous_by_id.get(&event.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&calendar_item_xml_with_change_key(
                                event,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&calendar_item_xml_with_change_key(
                                event,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&calendar_item_xml_with_change_key(
                                event,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let event_id = item.id;
                    if !current_set.contains(&event_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"event:{event_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("calendar", &collection_id, &current_items)
            }
            FolderKind::Tasks => {
                let collection_id = requested_sync_collection_id(request, "tasks", TASKS_FOLDER_ID);
                let tasks = self
                    .store
                    .fetch_accessible_tasks_in_collection(principal.account_id, &collection_id)
                    .await?;
                let sync_versions = sync_version_by_id(
                    self.store
                        .fetch_task_sync_versions(principal.account_id, &collection_id)
                        .await?,
                );
                let current_items = tasks
                    .iter()
                    .map(|task| {
                        (
                            task.id,
                            task_change_key(task, sync_versions.get(&task.id).map(String::as_str)),
                        )
                    })
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "tasks", &collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for task in &tasks {
                    let current_change_key =
                        task_change_key(task, sync_versions.get(&task.id).map(String::as_str));
                    match previous_by_id.get(&task.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&task_item_xml_with_change_key(
                                task,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&task_item_xml_with_change_key(
                                task,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&task_item_xml_with_change_key(
                                task,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let task_id = item.id;
                    if !current_set.contains(&task_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"task:{task_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("tasks", &collection_id, &current_items)
            }
            FolderKind::Mailbox => {
                let Some(mailbox_id) = self
                    .requested_mailbox_folder_ids(principal, request)
                    .await?
                    .into_iter()
                    .next()
                else {
                    return Ok(sync_folder_items_response("mailbox:0", String::new()));
                };
                let query = self
                    .store
                    .query_jmap_email_ids(
                        principal.account_id,
                        Some(mailbox_id),
                        None,
                        0,
                        MAILBOX_QUERY_LIMIT,
                    )
                    .await?;
                let emails = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &query.ids)
                    .await?
                    .into_iter()
                    .filter(|email| email.mailbox_id == mailbox_id)
                    .collect::<Vec<_>>();
                let current_ids = emails.iter().map(|email| email.id).collect::<Vec<_>>();
                let current_set = current_ids.iter().copied().collect::<HashSet<_>>();
                let previous_ids = requested_sync_state(request)
                    .map(|state| mailbox_sync_state_ids(&state, mailbox_id))
                    .unwrap_or_default();
                let previous_set = previous_ids.iter().copied().collect::<HashSet<_>>();

                for email in &emails {
                    if !previous_set.contains(&email.id) {
                        changes.push_str("<t:Create>");
                        changes.push_str(&message_summary_xml(email));
                        changes.push_str("</t:Create>");
                    }
                }
                for message_id in previous_ids {
                    if !current_set.contains(&message_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"message:{message_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                mailbox_sync_state(mailbox_id, &current_ids)
            }
        };

        Ok(sync_folder_items_response(&sync_state, changes))
    }

    async fn create_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            if element_content(request, "Contact").is_some() {
                let collection_id = requested_collection_id_in(request, "SavedItemFolderId");
                let contact = self
                    .store
                    .create_accessible_contact(
                        principal.account_id,
                        collection_id,
                        parse_create_contact_input(principal, request)?,
                    )
                    .await?;
                return Ok(create_contact_success_response(&contact));
            }
            if element_content(request, "CalendarItem").is_some() {
                let collection_id = requested_collection_id_in(request, "SavedItemFolderId");
                let event = self
                    .store
                    .create_accessible_event(
                        principal.account_id,
                        collection_id,
                        parse_create_event_input(principal, request)?,
                    )
                    .await?;
                return Ok(create_event_success_response(&event));
            }
            if element_content(request, "Task").is_some() {
                let task = self
                    .store
                    .create_accessible_task(
                        principal.account_id,
                        parse_create_task_input(principal, request)?,
                    )
                    .await?;
                return Ok(create_task_success_response(&task));
            }

            let input = parse_create_message_input(principal, request)?;
            let subject_for_audit = input.subject.clone();
            let disposition = attribute_value_after(request, "CreateItem", "MessageDisposition")
                .unwrap_or("SaveOnly");

            match disposition {
                "SaveOnly" => {
                    if let Some(mailbox_id) = self
                        .requested_mailbox_folder_ids(principal, request)
                        .await?
                        .into_iter()
                        .next()
                    {
                        let imported = self
                            .store
                            .import_jmap_email(
                                imported_email_input(input, mailbox_id),
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-import-custom-mailbox-message".to_string(),
                                    subject: subject_for_audit,
                                },
                            )
                            .await?;
                        return Ok(create_item_success_response(
                            imported.id,
                            &imported.delivery_status,
                        ));
                    }
                    let draft = self
                        .store
                        .save_draft_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-save-draft-message".to_string(),
                                subject: subject_for_audit,
                            },
                        )
                        .await?;
                    Ok(create_item_success_response(draft.message_id, "draft"))
                }
                "SendOnly" | "SendAndSaveCopy" => {
                    let submitted = self
                        .store
                        .submit_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-submit-message".to_string(),
                                subject: subject_for_audit,
                            },
                        )
                        .await?;
                    Ok(create_item_success_response(submitted.message_id, "queued"))
                }
                other => Ok(operation_error_response(
                    "CreateItem",
                    "ErrorInvalidOperation",
                    &format!("unsupported CreateItem MessageDisposition {other}"),
                )),
            }
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("CreateItem", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn update_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let contact_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("contact:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let event_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("event:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let task_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("task:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || contact_ids.len() + event_ids.len() + task_ids.len() + message_ids.len()
                    != ids.len()
            {
                return Ok(operation_error_response(
                    "UpdateItem",
                    "ErrorInvalidOperation",
                    "UpdateItem currently supports only contact, calendar, task, and read/flag message item ids.",
                ));
            }

            let mut items = String::new();
            if !message_ids.is_empty() {
                let Some((unread, flagged)) = parse_update_message_flags(request)? else {
                    return Ok(operation_error_response(
                        "UpdateItem",
                        "ErrorInvalidOperation",
                        "UpdateItem message updates currently support only IsRead and FlagStatus.",
                    ));
                };
                for message_id in message_ids {
                    let updated = self
                        .store
                        .update_jmap_email_flags(
                            principal.account_id,
                            message_id,
                            unread,
                            flagged,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-update-message-flags".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                    items.push_str(&message_item_xml(&updated));
                }
            }
            for contact_id in contact_ids {
                let existing = self
                    .store
                    .fetch_accessible_contacts_by_ids(principal.account_id, &[contact_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("contact not found"))?;
                let updated = self
                    .store
                    .update_accessible_contact(
                        principal.account_id,
                        contact_id,
                        parse_update_contact_input(principal, &existing, request),
                    )
                    .await?;
                items.push_str(&contact_item_xml(&updated));
            }
            for event_id in event_ids {
                let existing = self
                    .store
                    .fetch_accessible_events_by_ids(principal.account_id, &[event_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("event not found"))?;
                let updated = self
                    .store
                    .update_accessible_event(
                        principal.account_id,
                        event_id,
                        parse_update_event_input(principal, &existing, request)?,
                    )
                    .await?;
                items.push_str(&calendar_item_xml(&updated));
            }
            for task_id in task_ids {
                let existing = self
                    .store
                    .fetch_accessible_tasks_by_ids(principal.account_id, &[task_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("task not found"))?;
                let updated = self
                    .store
                    .update_accessible_task(
                        principal.account_id,
                        task_id,
                        parse_update_task_input(principal, &existing, request)?,
                    )
                    .await?;
                items.push_str(&task_item_xml(&updated));
            }

            Ok(update_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("UpdateItem", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn delete_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let contact_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("contact:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let event_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("event:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let task_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("task:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || contact_ids.len() + event_ids.len() + task_ids.len() + message_ids.len()
                    != ids.len()
            {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem currently supports only contact, calendar, task, and message ids.",
                ));
            }
            for contact_id in contact_ids {
                self.store
                    .delete_accessible_contact(principal.account_id, contact_id)
                    .await?;
            }
            for event_id in event_ids {
                self.store
                    .delete_accessible_event(principal.account_id, event_id)
                    .await?;
            }
            for task_id in task_ids {
                self.store
                    .delete_accessible_task(principal.account_id, task_id)
                    .await?;
            }
            let delete_type = attribute_value_after(request, "DeleteItem", "DeleteType")
                .unwrap_or("MoveToDeletedItems");
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            let trash_mailbox_id = mailboxes
                .iter()
                .find(|mailbox| mailbox.role == "trash")
                .map(|mailbox| mailbox.id);

            for message_id in message_ids {
                let existing = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &[message_id])
                    .await?;
                let Some(email) = existing.into_iter().next() else {
                    return Ok(operation_error_response(
                        "DeleteItem",
                        "ErrorItemNotFound",
                        "message not found",
                    ));
                };

                if delete_type == "HardDelete" || email.mailbox_role == "trash" {
                    self.store
                        .delete_jmap_email(
                            principal.account_id,
                            message_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-delete-message".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                } else if let Some(trash_mailbox_id) = trash_mailbox_id {
                    self.store
                        .move_jmap_email(
                            principal.account_id,
                            message_id,
                            trash_mailbox_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-move-message-to-trash".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                } else {
                    self.store
                        .delete_jmap_email(
                            principal.account_id,
                            message_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-delete-message-without-trash".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                }
            }

            Ok(delete_item_success_response())
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("DeleteItem", "ErrorItemNotFound", &error.to_string())
        }))
    }

    async fn move_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty() || message_ids.len() != ids.len() {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorInvalidOperation",
                    "MoveItem currently supports only canonical message ids.",
                ));
            }

            let target_mailbox_ids = self
                .requested_mailbox_folder_ids(principal, request)
                .await?;
            if target_mailbox_ids.len() != 1 {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorInvalidOperation",
                    "MoveItem requires exactly one canonical mailbox target folder.",
                ));
            }
            let target_mailbox_id = target_mailbox_ids[0];
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            if !mailboxes
                .iter()
                .any(|mailbox| mailbox.id == target_mailbox_id)
            {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorFolderNotFound",
                    "target mailbox folder not found",
                ));
            }

            let mut items = String::new();
            for message_id in message_ids {
                let moved = self
                    .store
                    .move_jmap_email(
                        principal.account_id,
                        message_id,
                        target_mailbox_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-move-message".to_string(),
                            subject: format!("{message_id}->{target_mailbox_id}"),
                        },
                    )
                    .await?;
                items.push_str(&message_item_xml(&moved));
            }

            Ok(move_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("MoveItem", "ErrorItemNotFound", &error.to_string())
        }))
    }

    async fn copy_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty() || message_ids.len() != ids.len() {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorInvalidOperation",
                    "CopyItem currently supports only canonical message ids.",
                ));
            }

            let target_mailbox_ids = self
                .requested_mailbox_folder_ids(principal, request)
                .await?;
            if target_mailbox_ids.len() != 1 {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorInvalidOperation",
                    "CopyItem requires exactly one canonical mailbox target folder.",
                ));
            }
            let target_mailbox_id = target_mailbox_ids[0];
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            if !mailboxes
                .iter()
                .any(|mailbox| mailbox.id == target_mailbox_id)
            {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorFolderNotFound",
                    "target mailbox folder not found",
                ));
            }

            let mut items = String::new();
            for message_id in message_ids {
                let copied = self
                    .store
                    .copy_jmap_email(
                        principal.account_id,
                        message_id,
                        target_mailbox_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-copy-message".to_string(),
                            subject: format!("{message_id}->{target_mailbox_id}"),
                        },
                    )
                    .await?;
                items.push_str(&message_item_xml(&copied));
            }

            Ok(copy_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("CopyItem", "ErrorItemNotFound", &error.to_string())
        }))
    }

    async fn requested_mailbox_folder_ids(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<Vec<Uuid>> {
        let mut ids = requested_mailbox_folder_ids(request);
        if ids.is_empty() {
            if let Some(mailbox_id) =
                requested_sync_state(request).and_then(|state| mailbox_sync_state_folder_id(&state))
            {
                ids.push(mailbox_id);
            }
        }
        if ids.is_empty() {
            if let Some(role) = requested_mailbox_role(request) {
                ids.extend(
                    self.store
                        .fetch_jmap_mailboxes(principal.account_id)
                        .await?
                        .into_iter()
                        .filter(|mailbox| mailbox.role == role)
                        .map(|mailbox| mailbox.id),
                );
            }
        }
        Ok(ids)
    }

    async fn create_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let display_name = element_text(request, "DisplayName")
                .ok_or_else(|| anyhow!("CreateFolder is missing DisplayName"))?;
            let mailbox = self
                .store
                .create_jmap_mailbox(
                    JmapMailboxCreateInput {
                        account_id: principal.account_id,
                        name: display_name.clone(),
                        sort_order: None,
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-create-folder".to_string(),
                        subject: display_name,
                    },
                )
                .await?;

            Ok(create_folder_success_response(&mailbox))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("CreateFolder", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn delete_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let folder_ids = requested_mailbox_folder_ids(request);
            if folder_ids.is_empty() {
                return Ok(operation_error_response(
                    "DeleteFolder",
                    "ErrorInvalidOperation",
                    "DeleteFolder currently supports only mailbox folder ids.",
                ));
            }

            for folder_id in folder_ids {
                self.store
                    .destroy_jmap_mailbox(
                        principal.account_id,
                        folder_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-delete-folder".to_string(),
                            subject: folder_id.to_string(),
                        },
                    )
                    .await?;
            }

            Ok(delete_folder_success_response())
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("DeleteFolder", "ErrorFolderNotFound", &error.to_string())
        }))
    }
}

fn decode_ews_body(headers: &HeaderMap, body: &[u8]) -> Result<String> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .chars()
        .filter(|character| {
            !character.is_ascii_whitespace() && *character != '"' && *character != '\''
        })
        .collect::<String>();

    if body.starts_with(&[0xff, 0xfe]) {
        return decode_utf16_body(&body[2..], true);
    }
    if body.starts_with(&[0xfe, 0xff]) {
        return decode_utf16_body(&body[2..], false);
    }
    if content_type.contains("charset=utf-16be") {
        return decode_utf16_body(body, false);
    }
    if content_type.contains("charset=utf-16le") || content_type.contains("charset=utf-16") {
        return decode_utf16_body(body, true);
    }

    std::str::from_utf8(body)
        .map(str::to_string)
        .map_err(|_| anyhow!("EWS request body is not valid UTF-8 or UTF-16"))
}

fn decode_utf16_body(body: &[u8], little_endian: bool) -> Result<String> {
    let mut chunks = body.chunks_exact(2);
    let words = chunks
        .by_ref()
        .map(|chunk| {
            if little_endian {
                u16::from_le_bytes([chunk[0], chunk[1]])
            } else {
                u16::from_be_bytes([chunk[0], chunk[1]])
            }
        })
        .collect::<Vec<_>>();
    if !chunks.remainder().is_empty() {
        return Err(anyhow!("EWS UTF-16 request body has an odd byte length"));
    }
    String::from_utf16(&words).map_err(|_| anyhow!("EWS request body is not valid UTF-16"))
}

#[derive(Debug, Clone)]
struct ParsedMailbox {
    address: String,
    display_name: Option<String>,
}

fn parse_create_message_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<SubmitMessageInput> {
    let message = element_content(request, "Message")
        .ok_or_else(|| anyhow!("CreateItem currently supports only Message items"))?;
    let body_tag = open_tag_text(message, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(message, "Body").unwrap_or_default();
    let body_text = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value
    };
    let from = element_content(message, "From").and_then(parse_first_mailbox);
    let sender = element_content(message, "Sender").and_then(parse_first_mailbox);
    let from_display = from
        .as_ref()
        .and_then(|mailbox| mailbox.display_name.clone())
        .or_else(|| Some(principal.display_name.clone()));
    let from_address = from
        .map(|mailbox| mailbox.address)
        .unwrap_or_else(|| principal.email.clone());

    Ok(SubmitMessageInput {
        draft_message_id: None,
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        source: "ews-createitem".to_string(),
        from_display,
        from_address,
        sender_display: sender
            .as_ref()
            .and_then(|mailbox| mailbox.display_name.clone()),
        sender_address: sender.map(|mailbox| mailbox.address),
        to: parse_recipients(message, "ToRecipients"),
        cc: parse_recipients(message, "CcRecipients"),
        bcc: parse_recipients(message, "BccRecipients"),
        subject: element_text(message, "Subject").unwrap_or_default(),
        body_text,
        body_html_sanitized: None,
        internet_message_id: element_text(message, "InternetMessageId"),
        mime_blob_ref: Some(format!("ews-createitem:{}", Uuid::new_v4())),
        size_octets: message.len() as i64,
        unread: Some(false),
        flagged: Some(false),
        attachments: Vec::new(),
    })
}

fn parse_create_contact_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertClientContactInput> {
    let contact = element_content(request, "Contact")
        .ok_or_else(|| anyhow!("CreateItem is missing Contact"))?;
    let email = contact_entry_value(contact, "EmailAddresses", "EmailAddress1")
        .or_else(|| element_text(contact, "EmailAddress"))
        .unwrap_or_else(|| principal.email.clone());
    let given_name = element_text(contact, "GivenName").unwrap_or_default();
    let surname = element_text(contact, "Surname").unwrap_or_default();
    let fallback_name = [given_name.as_str(), surname.as_str()]
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let name = element_text(contact, "DisplayName")
        .or_else(|| element_text(contact, "FileAs"))
        .or_else(|| (!fallback_name.trim().is_empty()).then_some(fallback_name))
        .unwrap_or_else(|| email.clone());
    let body_tag = open_tag_text(contact, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(contact, "Body").unwrap_or_default();
    let notes = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value
    };

    Ok(UpsertClientContactInput {
        id: None,
        account_id: principal.account_id,
        name,
        role: element_text(contact, "JobTitle").unwrap_or_default(),
        email,
        phone: contact_entry_value(contact, "PhoneNumbers", "MobilePhone")
            .or_else(|| contact_entry_value(contact, "PhoneNumbers", "BusinessPhone"))
            .or_else(|| contact_entry_value(contact, "PhoneNumbers", "HomePhone"))
            .unwrap_or_default(),
        team: element_text(contact, "CompanyName").unwrap_or_default(),
        notes,
    })
}

fn parse_update_contact_input(
    principal: &AccountPrincipal,
    existing: &AccessibleContact,
    request: &str,
) -> UpsertClientContactInput {
    let contact = element_content(request, "Contact").unwrap_or(request);
    let given_name = element_text(contact, "GivenName");
    let surname = element_text(contact, "Surname");
    let existing_given = first_name(&existing.name);
    let existing_surname = last_name(&existing.name);
    let name_from_parts = (given_name.is_some() || surname.is_some()).then(|| {
        [
            given_name.as_deref().unwrap_or(&existing_given),
            surname.as_deref().unwrap_or(&existing_surname),
        ]
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ")
    });
    let name = element_text(contact, "DisplayName")
        .or_else(|| element_text(contact, "FileAs"))
        .or(name_from_parts)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| existing.name.clone());
    let email = contact_entry_value(contact, "EmailAddresses", "EmailAddress1")
        .or_else(|| element_text(contact, "EmailAddress"))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| existing.email.clone());
    let notes = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(contact, "Body") {
        let body_tag = open_tag_text(contact, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.notes.clone()
    };

    UpsertClientContactInput {
        id: Some(existing.id),
        account_id: principal.account_id,
        name,
        role: deleted_or_updated_text(
            request,
            contact,
            "contacts:JobTitle",
            "JobTitle",
            &existing.role,
        ),
        email,
        phone: deleted_or_updated_contact_entry(
            request,
            contact,
            &[
                "contacts:PhoneNumber:MobilePhone",
                "contacts:PhoneNumber:BusinessPhone",
                "contacts:PhoneNumber:HomePhone",
            ],
            "PhoneNumbers",
            &["MobilePhone", "BusinessPhone", "HomePhone"],
            &existing.phone,
        ),
        team: deleted_or_updated_text(
            request,
            contact,
            "contacts:CompanyName",
            "CompanyName",
            &existing.team,
        ),
        notes,
    }
}

fn parse_create_event_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertClientEventInput> {
    let event = element_content(request, "CalendarItem")
        .ok_or_else(|| anyhow!("CreateItem is missing CalendarItem"))?;
    let start = element_text(event, "Start").unwrap_or_default();
    let end = element_text(event, "End").unwrap_or_default();
    let (date, time) = ews_datetime_parts(&start)
        .ok_or_else(|| anyhow!("CalendarItem is missing a valid Start value"))?;
    let duration_minutes = ews_duration_minutes(&start, &end).unwrap_or(60);
    let body_tag = open_tag_text(event, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(event, "Body").unwrap_or_default();
    let notes = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value
    };
    let (participants, _) = parse_event_participants(principal, event);

    Ok(UpsertClientEventInput {
        id: None,
        account_id: principal.account_id,
        date,
        time,
        time_zone: requested_time_zone(request).unwrap_or_else(|| "UTC".to_string()),
        duration_minutes,
        recurrence_rule: parse_ews_recurrence(event)?,
        title: element_text(event, "Subject").unwrap_or_else(|| "Untitled event".to_string()),
        location: element_text(event, "Location").unwrap_or_default(),
        attendees: calendar_attendee_labels(&participants),
        attendees_json: serialize_calendar_participants_metadata(&participants),
        notes,
    })
}

fn parse_update_event_input(
    principal: &AccountPrincipal,
    existing: &AccessibleEvent,
    request: &str,
) -> Result<UpsertClientEventInput> {
    let event = element_content(request, "CalendarItem").unwrap_or(request);
    let start = element_text(event, "Start");
    let end = element_text(event, "End");
    let (date, time) = start
        .as_deref()
        .and_then(ews_datetime_parts)
        .unwrap_or_else(|| (existing.date.clone(), existing.time.clone()));
    let duration_minutes = match (start.as_deref(), end.as_deref()) {
        (Some(start), Some(end)) => {
            ews_duration_minutes(start, end).unwrap_or(existing.duration_minutes)
        }
        (Some(start), None) => {
            ews_duration_minutes(start, &format!("{}T{}:00Z", existing.date, existing.time))
                .unwrap_or(existing.duration_minutes)
        }
        _ => existing.duration_minutes,
    };
    let notes = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(event, "Body") {
        let body_tag = open_tag_text(event, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.notes.clone()
    };
    let (participants, has_attendee_updates) = parse_event_participants(principal, event);

    Ok(UpsertClientEventInput {
        id: Some(existing.id),
        account_id: principal.account_id,
        date,
        time,
        time_zone: requested_time_zone(request).unwrap_or_else(|| existing.time_zone.clone()),
        duration_minutes,
        recurrence_rule: if field_deleted(request, "calendar:Recurrence") {
            String::new()
        } else if element_content(event, "Recurrence").is_some() {
            parse_ews_recurrence(event)?
        } else {
            existing.recurrence_rule.clone()
        },
        title: deleted_or_updated_text(
            request,
            event,
            "calendar:Subject",
            "Subject",
            &existing.title,
        )
        .if_empty(existing.title.clone()),
        location: deleted_or_updated_text(
            request,
            event,
            "calendar:Location",
            "Location",
            &existing.location,
        ),
        attendees: if has_attendee_updates {
            calendar_attendee_labels(&participants)
        } else {
            existing.attendees.clone()
        },
        attendees_json: if has_attendee_updates {
            serialize_calendar_participants_metadata(&participants)
        } else {
            existing.attendees_json.clone()
        },
        notes,
    })
}

fn parse_create_task_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertClientTaskInput> {
    let task =
        element_content(request, "Task").ok_or_else(|| anyhow!("CreateItem is missing Task"))?;
    let body_tag = open_tag_text(task, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(task, "Body").unwrap_or_default();
    let description = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value
    };
    let status = element_text(task, "Status")
        .map(|value| ews_task_status_to_canonical(&value))
        .transpose()?
        .unwrap_or("needs-action")
        .to_string();

    Ok(UpsertClientTaskInput {
        id: None,
        principal_account_id: principal.account_id,
        account_id: principal.account_id,
        task_list_id: requested_task_list_id(request)?,
        title: element_text(task, "Subject").unwrap_or_else(|| "Untitled task".to_string()),
        description,
        status,
        due_at: element_text(task, "DueDate"),
        completed_at: element_text(task, "CompleteDate"),
        sort_order: 0,
    })
}

fn parse_update_task_input(
    principal: &AccountPrincipal,
    existing: &ClientTask,
    request: &str,
) -> Result<UpsertClientTaskInput> {
    let task = element_content(request, "Task").unwrap_or(request);
    let description = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(task, "Body") {
        let body_tag = open_tag_text(task, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.description.clone()
    };
    let status = element_text(task, "Status")
        .map(|value| ews_task_status_to_canonical(&value))
        .transpose()?
        .unwrap_or(existing.status.as_str())
        .to_string();

    Ok(UpsertClientTaskInput {
        id: Some(existing.id),
        principal_account_id: principal.account_id,
        account_id: principal.account_id,
        task_list_id: requested_task_list_id(request)?.or(Some(existing.task_list_id)),
        title: deleted_or_updated_text(request, task, "task:Subject", "Subject", &existing.title)
            .if_empty(existing.title.clone()),
        description,
        status,
        due_at: if field_deleted(request, "task:DueDate") {
            None
        } else {
            element_text(task, "DueDate").or_else(|| existing.due_at.clone())
        },
        completed_at: if field_deleted(request, "task:CompleteDate") {
            None
        } else {
            element_text(task, "CompleteDate").or_else(|| existing.completed_at.clone())
        },
        sort_order: existing.sort_order,
    })
}

fn requested_task_list_id(request: &str) -> Result<Option<Uuid>> {
    match requested_collection_id(request) {
        Some("default") | Some("tasks") | None => Ok(None),
        Some(id) => Uuid::parse_str(id)
            .map(Some)
            .map_err(|_| anyhow!("Task folder id is not a canonical task-list id")),
    }
}

fn ews_task_status_to_canonical(value: &str) -> Result<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "notstarted" | "needs-action" => Ok("needs-action"),
        "inprogress" | "waitingonothers" | "in-progress" => Ok("in-progress"),
        "completed" => Ok("completed"),
        "deferred" | "cancelled" | "canceled" => Ok("cancelled"),
        other => bail!("unsupported task Status {other}"),
    }
}

#[derive(Debug, Clone)]
struct OofProjection {
    is_enabled: bool,
    text_body: String,
}

impl OofProjection {
    fn disabled() -> Self {
        Self {
            is_enabled: false,
            text_body: String::new(),
        }
    }
}

fn oof_projection_from_script(content: Option<&str>) -> OofProjection {
    let Some(content) = content else {
        return OofProjection::disabled();
    };
    let Ok(script) = lpe_core::sieve::parse_script(content) else {
        return OofProjection::disabled();
    };
    let Some(text_body) = find_vacation_reason(&script.statements) else {
        return OofProjection::disabled();
    };

    OofProjection {
        is_enabled: true,
        text_body,
    }
}

fn find_vacation_reason(statements: &[Statement]) -> Option<String> {
    for statement in statements {
        match statement {
            Statement::Action(Action::Vacation { reason, .. }) => return Some(reason.clone()),
            Statement::If {
                branches,
                else_block,
            } => {
                for (_, branch) in branches {
                    if let Some(reason) = find_vacation_reason(branch) {
                        return Some(reason);
                    }
                }
                if let Some(else_block) = else_block {
                    if let Some(reason) = find_vacation_reason(else_block) {
                        return Some(reason);
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn vacation_sieve_script(text_body: &str) -> String {
    let text_body = sieve_quote(text_body.trim());
    format!("require [\"vacation\"];\r\nvacation :days 7 \"{text_body}\";\r\n")
}

fn sieve_quote(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

trait EmptyStringFallback {
    fn if_empty(self, fallback: String) -> String;
}

impl EmptyStringFallback for String {
    fn if_empty(self, fallback: String) -> String {
        if self.trim().is_empty() {
            fallback
        } else {
            self
        }
    }
}

fn parse_event_participants(
    principal: &AccountPrincipal,
    event: &str,
) -> (CalendarParticipantsMetadata, bool) {
    let mut metadata = CalendarParticipantsMetadata {
        organizer: Some(CalendarOrganizerMetadata {
            email: principal.email.clone(),
            common_name: principal.display_name.clone(),
        }),
        attendees: Vec::new(),
    };
    let mut has_attendee_collections = false;
    for (collection_name, role) in [
        ("RequiredAttendees", "REQ-PARTICIPANT"),
        ("OptionalAttendees", "OPT-PARTICIPANT"),
    ] {
        let Some(collection) = element_content(event, collection_name) else {
            continue;
        };
        has_attendee_collections = true;
        metadata.attendees.extend(
            element_contents(collection, "Attendee")
                .into_iter()
                .filter_map(|attendee| parse_attendee(attendee, role)),
        );
    }
    (metadata, has_attendee_collections)
}

fn parse_attendee(attendee: &str, role: &str) -> Option<CalendarParticipantMetadata> {
    let mailbox = element_content(attendee, "Mailbox").and_then(parse_mailbox)?;
    Some(CalendarParticipantMetadata {
        email: mailbox.address,
        common_name: mailbox.display_name.unwrap_or_default(),
        role: role.to_string(),
        partstat: ews_response_type_to_partstat(&element_text(attendee, "ResponseType")),
        rsvp: false,
    })
}

fn ews_response_type_to_partstat(response_type: &Option<String>) -> String {
    match response_type
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "accept" => "accepted",
        "tentative" => "tentative",
        "decline" => "declined",
        _ => "needs-action",
    }
    .to_string()
}

fn parse_ews_recurrence(event: &str) -> Result<String> {
    let Some(recurrence) = element_content(event, "Recurrence") else {
        return Ok(String::new());
    };

    let mut parts = Vec::new();
    if let Some(daily) = element_content(recurrence, "DailyRecurrence") {
        parts.push("FREQ=DAILY".to_string());
        push_interval_part(&mut parts, daily);
    } else if let Some(weekly) = element_content(recurrence, "WeeklyRecurrence") {
        parts.push("FREQ=WEEKLY".to_string());
        push_interval_part(&mut parts, weekly);
        if let Some(days) = element_text(weekly, "DaysOfWeek") {
            let byday = days
                .split_whitespace()
                .map(ews_weekday_to_rrule)
                .collect::<Result<Vec<_>>>()?;
            if !byday.is_empty() {
                parts.push(format!("BYDAY={}", byday.join(",")));
            }
        }
    } else if let Some(monthly) = element_content(recurrence, "AbsoluteMonthlyRecurrence") {
        parts.push("FREQ=MONTHLY".to_string());
        push_interval_part(&mut parts, monthly);
        if let Some(day) = element_text(monthly, "DayOfMonth") {
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
        }
    } else if let Some(yearly) = element_content(recurrence, "AbsoluteYearlyRecurrence") {
        parts.push("FREQ=YEARLY".to_string());
        if let Some(day) = element_text(yearly, "DayOfMonth") {
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
        }
        if let Some(month) = element_text(yearly, "Month") {
            parts.push(format!("BYMONTH={}", ews_month_to_number(&month)?));
        }
    } else {
        bail!("unsupported EWS recurrence pattern");
    }

    if let Some(numbered) = element_content(recurrence, "NumberedRecurrence") {
        if let Some(count) = element_text(numbered, "NumberOfOccurrences") {
            parts.push(format!(
                "COUNT={}",
                parse_positive_number(&count, "NumberOfOccurrences")?
            ));
        }
    } else if let Some(end_date) = element_content(recurrence, "EndDateRecurrence") {
        if let Some(end) = element_text(end_date, "EndDate") {
            parts.push(format!("UNTIL={}", rrule_date(&end)?));
        }
    }

    Ok(parts.join(";"))
}

fn push_interval_part(parts: &mut Vec<String>, recurrence: &str) {
    if let Some(interval) = element_text(recurrence, "Interval")
        .and_then(|value| parse_positive_number(&value, "Interval").ok())
        .filter(|value| *value > 1)
    {
        parts.push(format!("INTERVAL={interval}"));
    }
}

fn parse_positive_number(value: &str, field: &str) -> Result<u32> {
    let number = value
        .trim()
        .parse::<u32>()
        .map_err(|_| anyhow!("{field} must be a positive integer"))?;
    if number == 0 {
        bail!("{field} must be a positive integer");
    }
    Ok(number)
}

fn ews_weekday_to_rrule(value: &str) -> Result<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "monday" => Ok("MO"),
        "tuesday" => Ok("TU"),
        "wednesday" => Ok("WE"),
        "thursday" => Ok("TH"),
        "friday" => Ok("FR"),
        "saturday" => Ok("SA"),
        "sunday" => Ok("SU"),
        other => bail!("unsupported recurrence weekday {other}"),
    }
}

fn ews_month_to_number(value: &str) -> Result<u32> {
    match value.trim().to_ascii_lowercase().as_str() {
        "january" => Ok(1),
        "february" => Ok(2),
        "march" => Ok(3),
        "april" => Ok(4),
        "may" => Ok(5),
        "june" => Ok(6),
        "july" => Ok(7),
        "august" => Ok(8),
        "september" => Ok(9),
        "october" => Ok(10),
        "november" => Ok(11),
        "december" => Ok(12),
        other => bail!("unsupported recurrence month {other}"),
    }
}

fn rrule_date(value: &str) -> Result<String> {
    let date = value.trim().split('T').next().unwrap_or_default();
    let mut parts = date.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        bail!("recurrence end date must be YYYY-MM-DD");
    };
    Ok(format!("{year}{month}{day}"))
}

fn parse_update_message_flags(request: &str) -> Result<Option<(Option<bool>, Option<bool>)>> {
    let unread = element_text(request, "IsRead")
        .map(|value| parse_xml_bool(&value).map(|is_read| !is_read))
        .transpose()?;
    let mut flagged = element_text(request, "FlagStatus")
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "notflagged" => Ok(false),
            "flagged" | "complete" => Ok(true),
            other => bail!("unsupported message FlagStatus {other}"),
        })
        .transpose()?;
    if field_deleted(request, "message:Flag") || field_deleted(request, "message:FlagStatus") {
        flagged = Some(false);
    }

    Ok((unread.is_some() || flagged.is_some()).then_some((unread, flagged)))
}

fn parse_xml_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => bail!("unsupported boolean value {other}"),
    }
}

fn requested_time_zone(request: &str) -> Option<String> {
    let time_zone = open_tag_text(request, "TimeZoneDefinition")?;
    attribute_value(time_zone, "Id").map(str::to_string)
}

fn requested_availability_window(request: &str) -> (Option<String>, Option<String>) {
    let time_window = element_content(request, "TimeWindow").unwrap_or(request);
    (
        element_text(time_window, "StartTime"),
        element_text(time_window, "EndTime"),
    )
}

fn event_overlaps_window(event: &AccessibleEvent, start: Option<&str>, end: Option<&str>) -> bool {
    let event_start = ews_datetime(&event.date, &event.time);
    let event_end = event_end_datetime(event);
    start.is_none_or(|start| event_end.as_str() > start)
        && end.is_none_or(|end| event_start.as_str() < end)
}

fn ews_datetime_parts(value: &str) -> Option<(String, String)> {
    let trimmed = value.trim();
    if trimmed.len() < 16 {
        return None;
    }
    let date = trimmed.get(0..10)?;
    let time = trimmed.get(11..16)?;
    Some((date.to_string(), time.to_string()))
}

fn ews_duration_minutes(start: &str, end: &str) -> Option<i32> {
    let (_, start_time) = ews_datetime_parts(start)?;
    let (_, end_time) = ews_datetime_parts(end)?;
    let start_minutes = time_minutes(&start_time)?;
    let end_minutes = time_minutes(&end_time)?;
    (end_minutes > start_minutes).then_some(end_minutes - start_minutes)
}

fn time_minutes(value: &str) -> Option<i32> {
    let (hour, minute) = value.split_once(':')?;
    Some(hour.parse::<i32>().ok()? * 60 + minute.parse::<i32>().ok()?)
}

fn contact_entry_value(contact: &str, collection_name: &str, key: &str) -> Option<String> {
    let collection = element_content(contact, collection_name)?;
    let mut rest = collection;
    while let Some(tag_start) = rest.find('<') {
        let raw_tag_text = &rest[tag_start + 1..];
        let tag_text = raw_tag_text.trim_start();
        let open_tag_start = tag_start + 1 + (raw_tag_text.len() - tag_text.len());
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let tag_end = tag_text.find('>')?;
        let open_tag = &tag_text[..tag_end];
        let qualified_name = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()?;
        let content_start = open_tag_start + tag_end + 1;
        if qualified_name.rsplit(':').next() == Some("Entry")
            && attribute_value(open_tag, "Key") == Some(key)
        {
            let close_pattern = format!("</{qualified_name}>");
            let content = &rest[content_start..];
            let content_end = content.find(&close_pattern)?;
            return Some(xml_text(&content[..content_end]));
        }
        rest = &rest[content_start..];
    }
    element_text(collection, "Entry")
}

fn deleted_or_updated_text(
    request: &str,
    container: &str,
    field_uri: &str,
    local_name: &str,
    existing: &str,
) -> String {
    if field_deleted(request, field_uri) {
        String::new()
    } else {
        element_text(container, local_name).unwrap_or_else(|| existing.to_string())
    }
}

fn deleted_or_updated_contact_entry(
    request: &str,
    contact: &str,
    field_uris: &[&str],
    collection_name: &str,
    keys: &[&str],
    existing: &str,
) -> String {
    if field_uris
        .iter()
        .any(|field_uri| field_deleted(request, field_uri))
    {
        return String::new();
    }
    keys.iter()
        .find_map(|key| contact_entry_value(contact, collection_name, key))
        .unwrap_or_else(|| existing.to_string())
}

fn field_deleted(request: &str, field_uri: &str) -> bool {
    element_contents(request, "DeleteItemField")
        .into_iter()
        .any(|delete| field_block_matches(delete, field_uri))
}

fn field_block_matches(block: &str, field_uri: &str) -> bool {
    if attribute_values_for_tag(block, "FieldURI", "FieldURI")
        .into_iter()
        .any(|value| value == field_uri)
    {
        return true;
    }

    let Some((base_field_uri, field_index)) = field_uri.rsplit_once(':') else {
        return false;
    };
    let indexed_fields = attribute_values_for_tag(block, "IndexedFieldURI", "FieldURI");
    let field_indexes = attribute_values_for_tag(block, "IndexedFieldURI", "FieldIndex");
    indexed_fields.iter().any(|value| *value == base_field_uri)
        && field_indexes.iter().any(|value| *value == field_index)
}

fn imported_email_input(input: SubmitMessageInput, mailbox_id: Uuid) -> JmapImportedEmailInput {
    JmapImportedEmailInput {
        account_id: input.account_id,
        submitted_by_account_id: input.submitted_by_account_id,
        mailbox_id,
        source: input.source,
        from_display: input.from_display,
        from_address: input.from_address,
        sender_display: input.sender_display,
        sender_address: input.sender_address,
        to: input.to,
        cc: input.cc,
        bcc: input.bcc,
        subject: input.subject,
        body_text: input.body_text,
        body_html_sanitized: input.body_html_sanitized,
        internet_message_id: input.internet_message_id,
        mime_blob_ref: input
            .mime_blob_ref
            .unwrap_or_else(|| format!("ews-createitem:{}", Uuid::new_v4())),
        size_octets: input.size_octets,
        received_at: None,
        attachments: input.attachments,
    }
}

fn parse_recipients(message: &str, collection_name: &str) -> Vec<SubmittedRecipientInput> {
    element_content(message, collection_name)
        .map(|collection| {
            element_contents(collection, "Mailbox")
                .into_iter()
                .filter_map(parse_mailbox)
                .map(|mailbox| SubmittedRecipientInput {
                    address: mailbox.address,
                    display_name: mailbox.display_name,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_file_attachment_upload(value: &str) -> Result<AttachmentUploadInput> {
    let file_name = element_text(value, "Name")
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| anyhow!("FileAttachment Name is required"))?;
    let media_type = element_text(value, "ContentType")
        .map(|content_type| content_type.trim().to_string())
        .filter(|content_type| !content_type.is_empty())
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let content = element_text(value, "Content")
        .map(|content| content.trim().to_string())
        .filter(|content| !content.is_empty())
        .ok_or_else(|| anyhow!("FileAttachment Content is required"))?;
    let blob_bytes = BASE64_STANDARD
        .decode(content.as_bytes())
        .map_err(|_| anyhow!("FileAttachment Content must be valid base64"))?;

    Ok(AttachmentUploadInput {
        file_name,
        media_type,
        blob_bytes,
    })
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

fn parse_first_mailbox(value: &str) -> Option<ParsedMailbox> {
    element_contents(value, "Mailbox")
        .into_iter()
        .find_map(parse_mailbox)
}

fn parse_mailbox(value: &str) -> Option<ParsedMailbox> {
    let address = element_text(value, "EmailAddress")?;
    if address.trim().is_empty() {
        return None;
    }
    Some(ParsedMailbox {
        address: address.trim().to_string(),
        display_name: element_text(value, "Name").filter(|name| !name.trim().is_empty()),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FolderKind {
    Root,
    Contacts,
    Calendar,
    Tasks,
    Mailbox,
}

fn operation_name(body: &str) -> Option<String> {
    let body_start = body.find(":Body").or_else(|| body.find("<Body"))?;
    let body_content_start = body[body_start..].find('>')? + body_start + 1;
    let mut remaining = &body[body_content_start..];

    loop {
        let tag_start = remaining.find('<')?;
        let tag_text = remaining[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') {
            return None;
        }
        if tag_text.starts_with('?') || tag_text.starts_with('!') {
            remaining = &tag_text[1..];
            continue;
        }

        let tag_end = tag_text
            .find(|value: char| value.is_whitespace() || value == '/' || value == '>')
            .unwrap_or(tag_text.len());
        let qualified_name = &tag_text[..tag_end];
        let local_name = qualified_name.rsplit(':').next()?;
        if local_name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_')
        {
            return Some(match local_name {
                "GetUserAvailabilityRequest" => "GetUserAvailability".to_string(),
                value if value.ends_with("Request") => {
                    value.trim_end_matches("Request").to_string()
                }
                _ => local_name.to_string(),
            });
        }
        return None;
    }
}

fn requested_folder_kind(request: &str) -> Option<FolderKind> {
    if let Some(kind) =
        requested_sync_state(request).and_then(|state| sync_state_folder_kind(&state))
    {
        return Some(kind);
    }
    if request.contains("DistinguishedFolderId Id=\"msgfolderroot\"")
        || request.contains("DistinguishedFolderId Id='msgfolderroot'")
        || request.contains("DistinguishedFolderId Id=\"root\"")
        || request.contains("DistinguishedFolderId Id='root'")
        || request.contains("FolderId Id=\"msgfolderroot\"")
        || request.contains("FolderId Id='msgfolderroot'")
        || request.contains("FolderId Id=\"root\"")
        || request.contains("FolderId Id='root'")
    {
        return Some(FolderKind::Root);
    }
    if request.contains("DistinguishedFolderId Id=\"calendar\"")
        || request.contains("DistinguishedFolderId Id='calendar'")
        || request.contains("FolderId Id=\"calendar\"")
        || request.contains("FolderId Id='calendar'")
    {
        return Some(FolderKind::Calendar);
    }
    if request.contains("DistinguishedFolderId Id=\"contacts\"")
        || request.contains("DistinguishedFolderId Id='contacts'")
        || request.contains("FolderId Id=\"contacts\"")
        || request.contains("FolderId Id='contacts'")
    {
        return Some(FolderKind::Contacts);
    }
    if request.contains("DistinguishedFolderId Id=\"tasks\"")
        || request.contains("DistinguishedFolderId Id='tasks'")
        || request.contains("FolderId Id=\"tasks\"")
        || request.contains("FolderId Id='tasks'")
    {
        return Some(FolderKind::Tasks);
    }
    if request.contains("mailbox:") || !requested_mailbox_folder_ids(request).is_empty() {
        return Some(FolderKind::Mailbox);
    }
    if requested_mailbox_role(request).is_some() {
        return Some(FolderKind::Mailbox);
    }
    requested_collection_id(request).and_then(|id| {
        if id.starts_with("shared-calendar-") {
            Some(FolderKind::Calendar)
        } else if id.starts_with("shared-contacts-") {
            Some(FolderKind::Contacts)
        } else if id.starts_with("shared-tasks-") {
            Some(FolderKind::Tasks)
        } else if id.starts_with("mailbox:") || Uuid::parse_str(id).is_ok() {
            Some(FolderKind::Mailbox)
        } else if id == "msgfolderroot" || id == "root" {
            Some(FolderKind::Root)
        } else {
            None
        }
    })
}

fn sync_state_folder_kind(sync_state: &str) -> Option<FolderKind> {
    if sync_state.starts_with("contacts:") {
        Some(FolderKind::Contacts)
    } else if sync_state.starts_with("calendar:") {
        Some(FolderKind::Calendar)
    } else if sync_state.starts_with("tasks:") {
        Some(FolderKind::Tasks)
    } else if sync_state.starts_with("mailbox:") {
        Some(FolderKind::Mailbox)
    } else if sync_state.starts_with("root:") {
        Some(FolderKind::Root)
    } else {
        None
    }
}

fn requested_folder_kinds(request: &str) -> Vec<FolderKind> {
    let mut kinds = Vec::new();
    if request.contains("DistinguishedFolderId Id=\"msgfolderroot\"")
        || request.contains("DistinguishedFolderId Id='msgfolderroot'")
        || request.contains("DistinguishedFolderId Id=\"root\"")
        || request.contains("DistinguishedFolderId Id='root'")
        || request.contains("FolderId Id=\"msgfolderroot\"")
        || request.contains("FolderId Id='msgfolderroot'")
        || request.contains("FolderId Id=\"root\"")
        || request.contains("FolderId Id='root'")
    {
        kinds.push(FolderKind::Root);
    }
    if request.contains("DistinguishedFolderId Id=\"contacts\"")
        || request.contains("DistinguishedFolderId Id='contacts'")
        || request.contains("FolderId Id=\"contacts\"")
        || request.contains("FolderId Id='contacts'")
        || request.contains("shared-contacts-")
    {
        kinds.push(FolderKind::Contacts);
    }
    if request.contains("DistinguishedFolderId Id=\"calendar\"")
        || request.contains("DistinguishedFolderId Id='calendar'")
        || request.contains("FolderId Id=\"calendar\"")
        || request.contains("FolderId Id='calendar'")
        || request.contains("shared-calendar-")
    {
        kinds.push(FolderKind::Calendar);
    }
    if request.contains("DistinguishedFolderId Id=\"tasks\"")
        || request.contains("DistinguishedFolderId Id='tasks'")
        || request.contains("FolderId Id=\"tasks\"")
        || request.contains("FolderId Id='tasks'")
        || request.contains("shared-tasks-")
    {
        kinds.push(FolderKind::Tasks);
    }
    if request.contains("mailbox:") || !requested_mailbox_folder_ids(request).is_empty() {
        kinds.push(FolderKind::Mailbox);
    }
    if requested_mailbox_role(request).is_some() {
        kinds.push(FolderKind::Mailbox);
    }
    kinds.dedup();
    kinds
}

fn request_contains_folder_reference(request: &str) -> bool {
    request.contains("FolderId") || request.contains("DistinguishedFolderId")
}

fn requested_collection_id(request: &str) -> Option<&str> {
    requested_collection_id_in(request, "")
}

fn requested_collection_id_in<'a>(request: &'a str, wrapper: &str) -> Option<&'a str> {
    let xml = if wrapper.is_empty() {
        request
    } else {
        element_content(request, wrapper)?
    };
    attribute_values_for_tag(xml, "FolderId", "Id")
        .into_iter()
        .next()
        .or_else(|| {
            attribute_values_for_tag(xml, "DistinguishedFolderId", "Id")
                .into_iter()
                .next()
        })
        .map(|value| match value {
            "contacts" | "calendar" | "tasks" => DEFAULT_COLLECTION_ID,
            other => other,
        })
}

fn requested_sync_collection_id(request: &str, kind: &str, default_id: &str) -> String {
    if let Some(collection_id) = requested_collection_id_in(request, "SyncFolderId") {
        return collection_id.to_string();
    }
    if let Some(sync_state) = requested_sync_state(request) {
        if let Some(collection_id) = collaboration_sync_state_collection_id(&sync_state, kind) {
            return collection_id.to_string();
        }
    }
    default_id.to_string()
}

fn requested_mailbox_role(request: &str) -> Option<&'static str> {
    requested_distinguished_folder_id(request).and_then(ews_distinguished_mailbox_role)
}

fn requested_distinguished_folder_id(request: &str) -> Option<&str> {
    attribute_values_for_tag(request, "DistinguishedFolderId", "Id")
        .into_iter()
        .next()
        .or_else(|| {
            attribute_values_for_tag(request, "FolderId", "Id")
                .into_iter()
                .next()
        })
}

fn ews_distinguished_mailbox_role(value: &str) -> Option<&'static str> {
    match value.to_ascii_lowercase().as_str() {
        "inbox" => Some("inbox"),
        "drafts" => Some("drafts"),
        "sentitems" | "sent" => Some("sent"),
        "deleteditems" | "trash" => Some("trash"),
        _ => None,
    }
}

fn requested_sync_state(request: &str) -> Option<String> {
    element_text(request, "SyncState").filter(|value| !value.trim().is_empty())
}

fn mailbox_sync_state(mailbox_id: Uuid, message_ids: &[Uuid]) -> String {
    format!(
        "mailbox:{mailbox_id}:{}",
        message_ids
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

const COLLABORATION_SYNC_STATE_VERSION: &str = "v2";

fn collaboration_sync_state(kind: &str, collection_id: &str, items: &[(Uuid, String)]) -> String {
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
struct SyncStateItem {
    id: Uuid,
    change_key: Option<String>,
}

#[derive(Debug, Clone)]
struct CollaborationSyncState {
    is_current_version: bool,
    items: Vec<SyncStateItem>,
}

impl Default for CollaborationSyncState {
    fn default() -> Self {
        Self {
            is_current_version: true,
            items: Vec::new(),
        }
    }
}

fn collaboration_sync_state_items(
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

fn collaboration_sync_state_collection_id<'a>(sync_state: &'a str, kind: &str) -> Option<&'a str> {
    sync_state
        .strip_prefix(&format!("{kind}:"))?
        .split(':')
        .next()
}

fn sync_state_items_by_id(items: &[SyncStateItem]) -> HashMap<Uuid, Option<String>> {
    items
        .iter()
        .map(|item| (item.id, item.change_key.clone()))
        .collect()
}

fn sync_version_by_id(items: Vec<(Uuid, String)>) -> HashMap<Uuid, String> {
    items.into_iter().collect()
}

fn mailbox_sync_state_ids(sync_state: &str, mailbox_id: Uuid) -> Vec<Uuid> {
    let prefix = format!("mailbox:{mailbox_id}:");
    sync_state
        .strip_prefix(&prefix)
        .unwrap_or_default()
        .split(',')
        .filter(|value| !value.is_empty() && *value != "0")
        .filter_map(|value| Uuid::parse_str(value).ok())
        .collect()
}

fn mailbox_sync_state_folder_id(sync_state: &str) -> Option<Uuid> {
    let rest = sync_state.strip_prefix("mailbox:")?;
    let folder_id = rest.split_once(':')?.0;
    Uuid::parse_str(folder_id).ok()
}

fn requested_item_ids(request: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut rest = request;
    while let Some(index) = rest.find("<t:ItemId").or_else(|| rest.find("<ItemId")) {
        rest = &rest[index..];
        if let Some(id) = attribute_value_after(rest, "ItemId", "Id") {
            ids.push(id.to_string());
        }
        rest = &rest[1..];
    }
    ids
}

fn requested_attachment_ids(request: &str) -> Vec<String> {
    attribute_values_for_tag(request, "AttachmentId", "Id")
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn requested_mime_content(request: &str) -> bool {
    request.contains("item:MimeContent") || request.contains("MimeContent")
}

fn requested_mailbox_folder_ids(request: &str) -> Vec<Uuid> {
    requested_folder_ids(request)
        .into_iter()
        .filter_map(|id| {
            id.strip_prefix("mailbox:")
                .or(Some(id.as_str()))
                .and_then(|value| Uuid::parse_str(value).ok())
        })
        .collect()
}

fn requested_folder_ids(request: &str) -> Vec<String> {
    attribute_values_for_tag(request, "FolderId", "Id")
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn attribute_values_for_tag<'a>(xml: &'a str, local_name: &str, attr: &str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut rest = xml;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let Some(tag_end) = tag_text.find('>') else {
            break;
        };
        let open_tag = &tag_text[..tag_end];
        let Some(qualified_name) = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()
        else {
            rest = &tag_text[tag_end + 1..];
            continue;
        };
        if qualified_name.rsplit(':').next() == Some(local_name) {
            if let Some(value) = attribute_value(open_tag, attr) {
                values.push(value);
            }
        }
        rest = &tag_text[tag_end + 1..];
    }
    values
}

fn attribute_value_after<'a>(body: &'a str, tag: &str, attr: &str) -> Option<&'a str> {
    let index = body.find(tag)?;
    let rest = &body[index..];
    let end = rest.find('>')?;
    let tag_text = &rest[..end];
    attribute_value(tag_text, attr)
}

fn attribute_value<'a>(tag_text: &'a str, attr: &str) -> Option<&'a str> {
    let pattern = format!("{attr}=");
    let start = tag_text.find(&pattern)? + pattern.len();
    let quote = tag_text[start..].chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value_start = start + quote.len_utf8();
    let value_end = tag_text[value_start..].find(quote)? + value_start;
    Some(&tag_text[value_start..value_end])
}

fn open_tag_text<'a>(xml: &'a str, local_name: &str) -> Option<&'a str> {
    let mut rest = xml;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let tag_end = tag_text.find('>')?;
        let open_tag = &tag_text[..tag_end];
        let qualified_name = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()?;
        if qualified_name.rsplit(':').next()? == local_name {
            return Some(open_tag);
        }
        rest = &tag_text[tag_end + 1..];
    }
    None
}

fn element_text(xml: &str, local_name: &str) -> Option<String> {
    element_content(xml, local_name).map(xml_text)
}

fn element_content<'a>(xml: &'a str, local_name: &str) -> Option<&'a str> {
    element_contents(xml, local_name).into_iter().next()
}

fn element_contents<'a>(xml: &'a str, local_name: &str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut rest = xml;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let Some(tag_end) = tag_text.find('>') else {
            break;
        };
        let open_tag = &tag_text[..tag_end];
        let Some(qualified_name) = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()
        else {
            break;
        };
        if qualified_name.rsplit(':').next() != Some(local_name) {
            rest = &tag_text[tag_end + 1..];
            continue;
        }
        if open_tag.trim_end().ends_with('/') {
            values.push("");
            rest = &tag_text[tag_end + 1..];
            continue;
        }

        let content_start = tag_start + 1 + tag_text[..tag_end + 1].len();
        let closing_tag = format!("</{qualified_name}>");
        let Some(relative_end) = rest[content_start..].find(&closing_tag) else {
            break;
        };
        let content_end = content_start + relative_end;
        values.push(&rest[content_start..content_end]);
        rest = &rest[content_end + closing_tag.len()..];
    }
    values
}

fn xml_text(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .trim()
        .to_string()
}

fn html_to_text(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut in_tag = false;
    for ch in value.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                output.push(' ');
            }
            _ if !in_tag => output.push(ch),
            _ => {}
        }
    }
    output.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn find_item_response(items: String) -> String {
    format!(
        concat!(
            "<m:FindItemResponse>",
            "<m:ResponseMessages>",
            "<m:FindItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:RootFolder TotalItemsInView=\"{count}\" IncludesLastItemInRange=\"true\">",
            "<t:Items>{items}</t:Items>",
            "</m:RootFolder>",
            "</m:FindItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:FindItemResponse>"
        ),
        items = items,
        count = count_tag_occurrences(&items, "<t:ItemId")
    )
}

fn sync_folder_items_response(sync_state: &str, changes: String) -> String {
    format!(
        concat!(
            "<m:SyncFolderItemsResponse>",
            "<m:ResponseMessages>",
            "<m:SyncFolderItemsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SyncState>{sync_state}</m:SyncState>",
            "<m:IncludesLastItemInRange>true</m:IncludesLastItemInRange>",
            "<m:Changes>{changes}</m:Changes>",
            "</m:SyncFolderItemsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:SyncFolderItemsResponse>"
        ),
        sync_state = escape_xml(sync_state),
        changes = changes,
    )
}

fn message_summary_xml(email: &JmapEmail) -> String {
    format!(
        concat!(
            "<t:Message>",
            "<t:ItemId Id=\"message:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"/>",
            "<t:Subject>{subject}</t:Subject>",
            "<t:DateTimeReceived>{received_at}</t:DateTimeReceived>",
            "<t:Size>{size}</t:Size>",
            "<t:HasAttachments>{has_attachments}</t:HasAttachments>",
            "<t:IsRead>{is_read}</t:IsRead>",
            "</t:Message>"
        ),
        id = email.id,
        change_key = escape_xml(&email.delivery_status),
        mailbox_id = email.mailbox_id,
        subject = escape_xml(&email.subject),
        received_at = escape_xml(&email.received_at),
        size = email.size_octets.max(0),
        has_attachments = email.has_attachments,
        is_read = !email.unread,
    )
}

fn message_item_xml(email: &JmapEmail) -> String {
    message_item_xml_with_attachments(email, &[])
}

fn message_item_xml_with_attachments(
    email: &JmapEmail,
    attachments: &[ActiveSyncAttachment],
) -> String {
    message_item_xml_with_details(email, attachments, None)
}

fn message_item_xml_with_details(
    email: &JmapEmail,
    attachments: &[ActiveSyncAttachment],
    mime_attachment_contents: Option<&[ActiveSyncAttachmentContent]>,
) -> String {
    let mut xml = message_summary_xml(email);
    let mime_content = mime_attachment_contents
        .map(|contents| {
            format!(
                "<t:MimeContent CharacterSet=\"UTF-8\">{}</t:MimeContent>",
                BASE64_STANDARD.encode(render_mime_message(email, contents))
            )
        })
        .unwrap_or_default();
    xml.insert_str(
        xml.len() - "</t:Message>".len(),
        &format!(
            "{}<t:Body BodyType=\"Text\">{}</t:Body>{}",
            mime_content,
            escape_xml(&email.body_text),
            message_attachments_xml(attachments),
        ),
    );
    xml
}

fn render_mime_message(email: &JmapEmail, attachments: &[ActiveSyncAttachmentContent]) -> Vec<u8> {
    let mut message = render_mime_header(email, attachments.is_empty());
    if attachments.is_empty() {
        message.push_str(&render_standalone_body_mime(email));
    } else {
        let boundary = mixed_boundary(email);
        message.push_str(&format!("--{boundary}\r\n"));
        message.push_str(&render_body_mime_part(email));
        if !message.ends_with("\r\n") {
            message.push_str("\r\n");
        }
        for attachment in attachments {
            message.push_str(&format!("--{boundary}\r\n"));
            message.push_str(&render_attachment_mime_part(attachment));
        }
        message.push_str(&format!("--{boundary}--\r\n"));
    }
    message.into_bytes()
}

fn render_standalone_body_mime(email: &JmapEmail) -> String {
    if let Some(html) = email.body_html_sanitized.as_deref() {
        let boundary = alternative_boundary(email);
        return format!(
            concat!(
                "--{boundary}\r\n",
                "Content-Type: text/plain; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{text}\r\n",
                "--{boundary}\r\n",
                "Content-Type: text/html; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{html}\r\n",
                "--{boundary}--\r\n"
            ),
            boundary = boundary,
            text = email.body_text,
            html = html,
        );
    }

    email.body_text.clone()
}

fn render_mime_header(email: &JmapEmail, without_attachments: bool) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "Date: {}",
        sanitize_header_value(&email.received_at)
    ));
    lines.push(format!(
        "From: {}",
        render_mime_address(email.from_display.as_deref(), email.from_address.as_str())
    ));
    if !email.to.is_empty() {
        lines.push(format!("To: {}", render_mime_recipients(&email.to)));
    }
    if !email.cc.is_empty() {
        lines.push(format!("Cc: {}", render_mime_recipients(&email.cc)));
    }
    if !email.bcc.is_empty() && matches!(email.mailbox_role.as_str(), "drafts" | "sent") {
        lines.push(format!("Bcc: {}", render_mime_recipients(&email.bcc)));
    }
    lines.push(format!(
        "Subject: {}",
        sanitize_header_value(&email.subject)
    ));
    if let Some(message_id) = email.internet_message_id.as_deref() {
        lines.push(format!("Message-Id: {}", sanitize_header_value(message_id)));
    }
    lines.push("MIME-Version: 1.0".to_string());
    let content_type = if without_attachments {
        body_content_type(email)
    } else {
        format!("multipart/mixed; boundary=\"{}\"", mixed_boundary(email))
    };
    lines.push(format!("Content-Type: {content_type}"));
    lines.join("\r\n") + "\r\n\r\n"
}

fn render_body_mime_part(email: &JmapEmail) -> String {
    if let Some(html) = email.body_html_sanitized.as_deref() {
        let boundary = alternative_boundary(email);
        return format!(
            concat!(
                "Content-Type: multipart/alternative; boundary=\"{boundary}\"\r\n",
                "\r\n",
                "--{boundary}\r\n",
                "Content-Type: text/plain; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{text}\r\n",
                "--{boundary}\r\n",
                "Content-Type: text/html; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{html}\r\n",
                "--{boundary}--\r\n"
            ),
            boundary = boundary,
            text = email.body_text,
            html = html,
        );
    }

    format!(
        concat!(
            "Content-Type: text/plain; charset=UTF-8\r\n",
            "Content-Transfer-Encoding: 7bit\r\n",
            "\r\n",
            "{}\r\n"
        ),
        email.body_text,
    )
}

fn render_attachment_mime_part(attachment: &ActiveSyncAttachmentContent) -> String {
    let file_name = quote_mime_parameter(&attachment.file_name);
    format!(
        concat!(
            "Content-Type: {content_type}; name=\"{file_name}\"\r\n",
            "Content-Transfer-Encoding: base64\r\n",
            "Content-Disposition: attachment; filename=\"{file_name}\"\r\n",
            "\r\n",
            "{body}\r\n"
        ),
        content_type = sanitize_header_value(&attachment.media_type),
        file_name = file_name,
        body = base64_mime_lines(&attachment.blob_bytes),
    )
}

fn body_content_type(email: &JmapEmail) -> String {
    if email.body_html_sanitized.is_some() {
        format!(
            "multipart/alternative; boundary=\"{}\"",
            alternative_boundary(email)
        )
    } else {
        "text/plain; charset=UTF-8".to_string()
    }
}

fn mixed_boundary(email: &JmapEmail) -> String {
    format!("lpe-ews-mixed-{}", email.id.simple())
}

fn alternative_boundary(email: &JmapEmail) -> String {
    format!("lpe-ews-alt-{}", email.id.simple())
}

fn render_mime_recipients(recipients: &[JmapEmailAddress]) -> String {
    recipients
        .iter()
        .map(|recipient| render_mime_address(recipient.display_name.as_deref(), &recipient.address))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_mime_address(display_name: Option<&str>, address: &str) -> String {
    let address = sanitize_header_value(address);
    match display_name
        .map(sanitize_header_value)
        .filter(|value| !value.trim().is_empty() && value != &address)
    {
        Some(display_name) => format!("{} <{}>", quote_display_name(&display_name), address),
        None => address,
    }
}

fn quote_display_name(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ' ' | '.' | '_' | '-'))
    {
        value.to_string()
    } else {
        format!("\"{}\"", quote_mime_parameter(value))
    }
}

fn quote_mime_parameter(value: &str) -> String {
    sanitize_header_value(value)
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn sanitize_header_value(value: &str) -> String {
    value
        .replace(['\r', '\n'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn base64_mime_lines(bytes: &[u8]) -> String {
    bytes
        .chunks(57)
        .map(|chunk| BASE64_STANDARD.encode(chunk))
        .collect::<Vec<_>>()
        .join("\r\n")
}

fn message_attachments_xml(attachments: &[ActiveSyncAttachment]) -> String {
    if attachments.is_empty() {
        return String::new();
    }

    format!(
        "<t:Attachments>{}</t:Attachments>",
        attachments
            .iter()
            .map(file_attachment_reference_xml)
            .collect::<String>()
    )
}

fn file_attachment_reference_xml(attachment: &ActiveSyncAttachment) -> String {
    format!(
        concat!(
            "<t:FileAttachment>",
            "<t:AttachmentId Id=\"{file_reference}\"/>",
            "<t:Name>{name}</t:Name>",
            "<t:ContentType>{content_type}</t:ContentType>",
            "<t:Size>{size}</t:Size>",
            "<t:IsInline>false</t:IsInline>",
            "</t:FileAttachment>"
        ),
        file_reference = escape_xml(&attachment.file_reference),
        name = escape_xml(&attachment.file_name),
        content_type = escape_xml(&attachment.media_type),
        size = attachment.size_octets,
    )
}

fn file_attachment_content_xml(content: &ActiveSyncAttachmentContent) -> String {
    format!(
        concat!(
            "<t:FileAttachment>",
            "<t:AttachmentId Id=\"{file_reference}\"/>",
            "<t:Name>{name}</t:Name>",
            "<t:ContentType>{content_type}</t:ContentType>",
            "<t:Size>{size}</t:Size>",
            "<t:IsInline>false</t:IsInline>",
            "<t:Content>{body}</t:Content>",
            "</t:FileAttachment>"
        ),
        file_reference = escape_xml(&content.file_reference),
        name = escape_xml(&content.file_name),
        content_type = escape_xml(&content.media_type),
        size = content.blob_bytes.len(),
        body = BASE64_STANDARD.encode(&content.blob_bytes),
    )
}

fn root_item_id_xml(email: &JmapEmail) -> String {
    format!(
        "<m:RootItemId RootItemId=\"message:{id}\" RootItemChangeKey=\"{change_key}\"/>",
        id = email.id,
        change_key = escape_xml(&email.delivery_status),
    )
}

fn create_item_success_response(message_id: Uuid, delivery_status: &str) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Message>",
            "<t:ItemId Id=\"message:{message_id}\" ChangeKey=\"{delivery_status}\"/>",
            "</t:Message>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        message_id = message_id,
        delivery_status = escape_xml(delivery_status),
    )
}

fn create_contact_success_response(contact: &AccessibleContact) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\" ChangeKey=\"created\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "</t:Contact>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = contact.id,
        folder_id = escape_xml(&contact.collection_id),
        name = escape_xml(&contact.name),
    )
}

fn create_event_success_response(event: &AccessibleEvent) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"created\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "</t:CalendarItem>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = event.id,
        folder_id = escape_xml(&event.collection_id),
        title = escape_xml(&event.title),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
    )
}

fn create_task_success_response(task: &ClientTask) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Task>",
            "<t:ItemId Id=\"task:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Status>{status}</t:Status>",
            "{due_date}",
            "{complete_date}",
            "</t:Task>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = task.id,
        change_key = escape_xml(&task_change_key(task, None)),
        folder_id = task.task_list_id,
        title = escape_xml(&task.title),
        status = ews_task_status(&task.status),
        due_date = optional_text_element("t:DueDate", task.due_at.as_deref()),
        complete_date = optional_text_element("t:CompleteDate", task.completed_at.as_deref()),
    )
}

fn update_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:UpdateItemResponse>",
            "<m:ResponseMessages>",
            "<m:UpdateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:UpdateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:UpdateItemResponse>"
        ),
        items = items,
    )
}

fn delete_item_success_response() -> String {
    concat!(
        "<m:DeleteItemResponse>",
        "<m:ResponseMessages>",
        "<m:DeleteItemResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:DeleteItemResponseMessage>",
        "</m:ResponseMessages>",
        "</m:DeleteItemResponse>"
    )
    .to_string()
}

fn move_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:MoveItemResponse>",
            "<m:ResponseMessages>",
            "<m:MoveItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:MoveItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:MoveItemResponse>"
        ),
        items = items,
    )
}

fn copy_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:CopyItemResponse>",
            "<m:ResponseMessages>",
            "<m:CopyItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:CopyItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CopyItemResponse>"
        ),
        items = items,
    )
}

fn get_attachment_success_response(attachments: String) -> String {
    format!(
        concat!(
            "<m:GetAttachmentResponse>",
            "<m:ResponseMessages>",
            "<m:GetAttachmentResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Attachments>{attachments}</m:Attachments>",
            "</m:GetAttachmentResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetAttachmentResponse>"
        ),
        attachments = attachments,
    )
}

fn create_attachment_success_response(attachments: String, root_item: String) -> String {
    format!(
        concat!(
            "<m:CreateAttachmentResponse>",
            "<m:ResponseMessages>",
            "<m:CreateAttachmentResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Attachments>{attachments}</m:Attachments>",
            "{root_item}",
            "</m:CreateAttachmentResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateAttachmentResponse>"
        ),
        attachments = attachments,
        root_item = root_item,
    )
}

fn delete_attachment_success_response(root_items: String) -> String {
    format!(
        concat!(
            "<m:DeleteAttachmentResponse>",
            "<m:ResponseMessages>",
            "<m:DeleteAttachmentResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{root_items}",
            "</m:DeleteAttachmentResponseMessage>",
            "</m:ResponseMessages>",
            "</m:DeleteAttachmentResponse>"
        ),
        root_items = root_items,
    )
}

fn create_folder_success_response(mailbox: &JmapMailbox) -> String {
    format!(
        concat!(
            "<m:CreateFolderResponse>",
            "<m:ResponseMessages>",
            "<m:CreateFolderResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Folders>{folder}</m:Folders>",
            "</m:CreateFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateFolderResponse>"
        ),
        folder = mailbox_folder_xml(mailbox),
    )
}

fn delete_folder_success_response() -> String {
    concat!(
        "<m:DeleteFolderResponse>",
        "<m:ResponseMessages>",
        "<m:DeleteFolderResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:DeleteFolderResponseMessage>",
        "</m:ResponseMessages>",
        "</m:DeleteFolderResponse>"
    )
    .to_string()
}

fn get_item_error_response(code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:GetItemResponse>",
            "<m:ResponseMessages>",
            "<m:GetItemResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "<m:Items/>",
            "</m:GetItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetItemResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

fn get_folder_error_response(code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:GetFolderResponse>",
            "<m:ResponseMessages>",
            "<m:GetFolderResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "<m:Folders/>",
            "</m:GetFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetFolderResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

fn get_server_time_zones_response() -> String {
    concat!(
        "<m:GetServerTimeZonesResponse>",
        "<m:ResponseMessages>",
        "<m:GetServerTimeZonesResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "<m:TimeZoneDefinitions>",
        "<t:TimeZoneDefinition Id=\"UTC\" Name=\"(UTC) Coordinated Universal Time\"/>",
        "<t:TimeZoneDefinition Id=\"W. Europe Standard Time\" Name=\"(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna\"/>",
        "</m:TimeZoneDefinitions>",
        "</m:GetServerTimeZonesResponseMessage>",
        "</m:ResponseMessages>",
        "</m:GetServerTimeZonesResponse>"
    )
    .to_string()
}

fn resolve_names_no_results_response() -> String {
    concat!(
        "<m:ResolveNamesResponse>",
        "<m:ResponseMessages>",
        "<m:ResolveNamesResponseMessage ResponseClass=\"Error\">",
        "<m:MessageText>No results were found.</m:MessageText>",
        "<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>",
        "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
        "</m:ResolveNamesResponseMessage>",
        "</m:ResponseMessages>",
        "</m:ResolveNamesResponse>"
    )
    .to_string()
}

fn resolve_names_response(
    principal: &AccountPrincipal,
    request: &str,
    entries: &[ExchangeAddressBookEntry],
) -> String {
    let query = element_text(request, "UnresolvedEntry")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if query.is_empty() {
        return resolve_names_no_results_response();
    }
    let principal_entry = principal_address_book_entry(principal);
    let matched = entries
        .iter()
        .find(|entry| address_book_entry_matches(entry, &query, true))
        .or_else(|| {
            address_book_lookup_matches_principal(&query, principal).then_some(&principal_entry)
        });
    let Some(entry) = matched else {
        return resolve_names_no_results_response();
    };

    format!(
        concat!(
            "<m:ResolveNamesResponse>",
            "<m:ResponseMessages>",
            "<m:ResolveNamesResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:ResolutionSet TotalItemsInView=\"1\" IncludesLastItemInRange=\"true\">",
            "<t:Resolution>",
            "<t:Mailbox>",
            "<t:Name>{}</t:Name>",
            "<t:EmailAddress>{}</t:EmailAddress>",
            "<t:RoutingType>SMTP</t:RoutingType>",
            "<t:MailboxType>{}</t:MailboxType>",
            "</t:Mailbox>",
            "</t:Resolution>",
            "</m:ResolutionSet>",
            "</m:ResolveNamesResponseMessage>",
            "</m:ResponseMessages>",
            "</m:ResolveNamesResponse>"
        ),
        escape_xml(&entry.display_name),
        escape_xml(&entry.email),
        ews_mailbox_type(entry),
    )
}

fn principal_address_book_entry(principal: &AccountPrincipal) -> ExchangeAddressBookEntry {
    ExchangeAddressBookEntry {
        id: principal.account_id,
        display_name: principal.display_name.clone(),
        email: principal.email.clone(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
    }
}

fn address_book_lookup_matches_principal(value: &str, principal: &AccountPrincipal) -> bool {
    let value = normalize_address_book_lookup(value);
    let email = principal.email.to_ascii_lowercase();
    let display_name = principal.display_name.to_ascii_lowercase();
    value == email || value == display_name || email.contains(value.as_str())
}

fn address_book_entry_matches(
    entry: &ExchangeAddressBookEntry,
    value: &str,
    allow_partial: bool,
) -> bool {
    let value = normalize_address_book_lookup(value);
    if value.is_empty() {
        return false;
    }
    let email = entry.email.to_ascii_lowercase();
    let display_name = entry.display_name.to_ascii_lowercase();
    value == email
        || value == display_name
        || value == format!("smtp:{email}")
        || value == format!("=smtp:{email}")
        || (allow_partial
            && (email.contains(value.as_str()) || display_name.contains(value.as_str())))
}

fn normalize_address_book_lookup(value: &str) -> String {
    let mut value = value.trim().trim_matches('\0').to_ascii_lowercase();
    if let Some(rest) = value.strip_prefix("=smtp:") {
        value = rest.to_string();
    } else if let Some(rest) = value.strip_prefix("smtp:") {
        value = rest.to_string();
    }
    value
}

fn ews_mailbox_type(entry: &ExchangeAddressBookEntry) -> &'static str {
    match entry.entry_kind {
        ExchangeAddressBookEntryKind::Contact => "Contact",
        ExchangeAddressBookEntryKind::Account => "Mailbox",
    }
}

fn get_user_availability_success_response(events: &[AccessibleEvent]) -> String {
    let events = events
        .iter()
        .map(|event| {
            format!(
                concat!(
                    "<t:CalendarEvent>",
                    "<t:StartTime>{}</t:StartTime>",
                    "<t:EndTime>{}</t:EndTime>",
                    "<t:BusyType>Busy</t:BusyType>",
                    "</t:CalendarEvent>"
                ),
                escape_xml(&ews_datetime(&event.date, &event.time)),
                escape_xml(&event_end_datetime(event)),
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetUserAvailabilityResponse>",
            "<m:FreeBusyResponseArray>",
            "<m:FreeBusyResponse>",
            "<m:ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "</m:ResponseMessage>",
            "<m:FreeBusyView>",
            "<t:FreeBusyViewType>Detailed</t:FreeBusyViewType>",
            "<t:CalendarEventArray>{events}</t:CalendarEventArray>",
            "</m:FreeBusyView>",
            "</m:FreeBusyResponse>",
            "</m:FreeBusyResponseArray>",
            "</m:GetUserAvailabilityResponse>"
        ),
        events = events,
    )
}

fn get_user_availability_error_response(message: &str) -> String {
    format!(
        concat!(
            "<m:GetUserAvailabilityResponse>",
            "<m:FreeBusyResponseArray>",
            "<m:FreeBusyResponse>",
            "<m:ResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>ErrorFreeBusyGenerationFailed</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "</m:ResponseMessage>",
            "</m:FreeBusyResponse>",
            "</m:FreeBusyResponseArray>",
            "</m:GetUserAvailabilityResponse>"
        ),
        message = escape_xml(message),
    )
}

fn get_user_oof_settings_response(projection: &OofProjection) -> String {
    let state = if projection.is_enabled {
        "Enabled"
    } else {
        "Disabled"
    };
    let audience = if projection.is_enabled { "All" } else { "None" };
    let message = escape_xml(&projection.text_body);
    format!(
        concat!(
            "<m:GetUserOofSettingsResponse>",
            "<m:ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "</m:ResponseMessage>",
            "<m:OofSettings>",
            "<t:OofState>{state}</t:OofState>",
            "<t:ExternalAudience>{audience}</t:ExternalAudience>",
            "<t:InternalReply><t:Message>{message}</t:Message></t:InternalReply>",
            "<t:ExternalReply><t:Message>{message}</t:Message></t:ExternalReply>",
            "</m:OofSettings>",
            "<m:AllowExternalOof>{audience}</m:AllowExternalOof>",
            "</m:GetUserOofSettingsResponse>"
        ),
        state = state,
        audience = audience,
        message = message,
    )
}

fn set_user_oof_settings_success_response() -> String {
    concat!(
        "<m:SetUserOofSettingsResponse>",
        "<m:ResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:ResponseMessage>",
        "</m:SetUserOofSettingsResponse>"
    )
    .to_string()
}

fn unsupported_operation_response(operation: &str) -> String {
    operation_error_response(
        operation,
        "ErrorInvalidOperation",
        &format!("{operation} is not implemented by the EWS MVP."),
    )
}

fn operation_error_response(operation: &str, code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = escape_xml(operation),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

fn root_folder_xml(child_folder_count: usize) -> String {
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>",
            "<t:FolderClass>IPF.Note</t:FolderClass>",
            "<t:DisplayName>Root</t:DisplayName>",
            "<t:TotalCount>0</t:TotalCount>",
            "<t:ChildFolderCount>{child_folder_count}</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>true</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>0</t:UnreadCount>",
            "</t:Folder>"
        ),
        child_folder_count = child_folder_count,
    )
}

fn folder_xml(collection: &CollaborationCollection, distinguished_id: &str, class: &str) -> String {
    let element = match distinguished_id {
        CONTACTS_FOLDER_ID => "ContactsFolder",
        CALENDAR_FOLDER_ID => "CalendarFolder",
        TASKS_FOLDER_ID => "TasksFolder",
        _ => "Folder",
    };
    format!(
        concat!(
            "<t:{element}>",
            "<t:FolderId Id=\"{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>",
            "<t:FolderClass>IPF.{class}</t:FolderClass>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:TotalCount>0</t:TotalCount>",
            "<t:ChildFolderCount>0</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>true</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>0</t:UnreadCount>",
            "</t:{element}>"
        ),
        element = element,
        id = escape_xml(&collection.id),
        change_key = escape_xml(&folder_change_key(&collection.id)),
        display = escape_xml(&collection.display_name),
        class = class,
    )
}

fn mailbox_folder_xml(mailbox: &JmapMailbox) -> String {
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"mailbox:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>",
            "<t:FolderClass>IPF.Note</t:FolderClass>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:TotalCount>{total_count}</t:TotalCount>",
            "<t:ChildFolderCount>0</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>true</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>{unread_count}</t:UnreadCount>",
            "</t:Folder>"
        ),
        id = mailbox.id,
        change_key = folder_change_key(&mailbox.id.to_string()),
        display = escape_xml(&mailbox.name),
        total_count = mailbox.total_emails,
        unread_count = mailbox.unread_emails,
    )
}

fn folder_change_key(id: &str) -> String {
    format!("ck-{id}")
}

fn contact_change_key(contact: &AccessibleContact, sync_version: Option<&str>) -> String {
    stable_change_key(&[
        "contact",
        &contact.id.to_string(),
        sync_version.unwrap_or_default(),
        &contact.collection_id,
        &contact.name,
        &contact.role,
        &contact.email,
        &contact.phone,
        &contact.team,
        &contact.notes,
    ])
}

fn calendar_change_key(event: &AccessibleEvent, sync_version: Option<&str>) -> String {
    stable_change_key(&[
        "calendar",
        &event.id.to_string(),
        sync_version.unwrap_or_default(),
        &event.collection_id,
        &event.date,
        &event.time,
        &event.time_zone,
        &event.duration_minutes.to_string(),
        &event.recurrence_rule,
        &event.title,
        &event.location,
        &event.attendees,
        &event.attendees_json,
        &event.notes,
    ])
}

fn task_change_key(task: &ClientTask, sync_version: Option<&str>) -> String {
    stable_change_key(&[
        "task",
        &task.id.to_string(),
        sync_version.unwrap_or_default(),
        &task.task_list_id.to_string(),
        &task.title,
        &task.description,
        &task.status,
        task.due_at.as_deref().unwrap_or_default(),
        task.completed_at.as_deref().unwrap_or_default(),
        &task.sort_order.to_string(),
    ])
}

fn stable_change_key(parts: &[&str]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("ck-{hash:016x}")
}

fn count_folder_elements(value: &str) -> usize {
    count_tag_occurrences(value, "<t:Folder>")
        + count_tag_occurrences(value, "<t:ContactsFolder>")
        + count_tag_occurrences(value, "<t:CalendarFolder>")
        + count_tag_occurrences(value, "<t:TasksFolder>")
}

fn contact_summary_xml(contact: &AccessibleContact) -> String {
    let change_key = contact_change_key(contact, None);
    contact_summary_xml_with_change_key(contact, &change_key)
}

fn contact_summary_xml_with_change_key(contact: &AccessibleContact, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "</t:Contact>"
        ),
        id = contact.id,
        change_key = escape_xml(change_key),
        name = escape_xml(&contact.name),
    )
}

fn contact_item_xml(contact: &AccessibleContact) -> String {
    let change_key = contact_change_key(contact, None);
    contact_item_xml_with_change_key(contact, &change_key)
}

fn contact_item_xml_with_change_key(contact: &AccessibleContact, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "<t:GivenName>{given}</t:GivenName>",
            "<t:Surname>{surname}</t:Surname>",
            "<t:JobTitle>{role}</t:JobTitle>",
            "<t:CompanyName>{team}</t:CompanyName>",
            "<t:EmailAddresses><t:Entry Key=\"EmailAddress1\">{email}</t:Entry></t:EmailAddresses>",
            "<t:PhoneNumbers><t:Entry Key=\"MobilePhone\">{phone}</t:Entry></t:PhoneNumbers>",
            "<t:Body BodyType=\"Text\">{notes}</t:Body>",
            "</t:Contact>"
        ),
        id = contact.id,
        change_key = escape_xml(change_key),
        folder_id = escape_xml(&contact.collection_id),
        name = escape_xml(&contact.name),
        given = escape_xml(&first_name(&contact.name)),
        surname = escape_xml(&last_name(&contact.name)),
        role = escape_xml(&contact.role),
        team = escape_xml(&contact.team),
        email = escape_xml(&contact.email),
        phone = escape_xml(&contact.phone),
        notes = escape_xml(&contact.notes),
    )
}

fn calendar_item_summary_xml(event: &AccessibleEvent) -> String {
    let change_key = calendar_change_key(event, None);
    calendar_item_summary_xml_with_change_key(event, &change_key)
}

fn calendar_item_summary_xml_with_change_key(event: &AccessibleEvent, change_key: &str) -> String {
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        change_key = escape_xml(change_key),
        title = escape_xml(&event.title),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
    )
}

fn calendar_item_xml(event: &AccessibleEvent) -> String {
    let change_key = calendar_change_key(event, None);
    calendar_item_xml_with_change_key(event, &change_key)
}

fn calendar_item_xml_with_change_key(event: &AccessibleEvent, change_key: &str) -> String {
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Location>{location}</t:Location>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "{recurrence}",
            "<t:LegacyFreeBusyStatus>Busy</t:LegacyFreeBusyStatus>",
            "{attendees}",
            "<t:Body BodyType=\"Text\">{notes}</t:Body>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        change_key = escape_xml(change_key),
        folder_id = escape_xml(&event.collection_id),
        title = escape_xml(&event.title),
        location = escape_xml(&event.location),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
        recurrence = ews_recurrence_xml(event),
        attendees = ews_attendees_xml(event),
        notes = escape_xml(&event.notes),
    )
}

fn ews_attendees_xml(event: &AccessibleEvent) -> String {
    let metadata = parse_calendar_participants_metadata(&event.attendees_json);
    let required = ews_attendee_collection_xml(
        "RequiredAttendees",
        metadata
            .attendees
            .iter()
            .filter(|attendee| !attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT")),
    );
    let optional = ews_attendee_collection_xml(
        "OptionalAttendees",
        metadata
            .attendees
            .iter()
            .filter(|attendee| attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT")),
    );
    format!("{required}{optional}")
}

fn ews_attendee_collection_xml<'a>(
    element_name: &str,
    attendees: impl Iterator<Item = &'a CalendarParticipantMetadata>,
) -> String {
    let attendees = attendees.map(ews_attendee_xml).collect::<String>();
    if attendees.is_empty() {
        String::new()
    } else {
        format!("<t:{element_name}>{attendees}</t:{element_name}>")
    }
}

fn ews_attendee_xml(attendee: &CalendarParticipantMetadata) -> String {
    format!(
        concat!(
            "<t:Attendee>",
            "<t:Mailbox>",
            "<t:Name>{}</t:Name>",
            "<t:EmailAddress>{}</t:EmailAddress>",
            "<t:RoutingType>SMTP</t:RoutingType>",
            "</t:Mailbox>",
            "<t:ResponseType>{}</t:ResponseType>",
            "</t:Attendee>"
        ),
        escape_xml(&attendee.common_name),
        escape_xml(&attendee.email),
        partstat_to_ews_response_type(&attendee.partstat),
    )
}

fn partstat_to_ews_response_type(partstat: &str) -> &'static str {
    match partstat.trim().to_ascii_lowercase().as_str() {
        "accepted" => "Accept",
        "tentative" => "Tentative",
        "declined" => "Decline",
        _ => "NoResponseReceived",
    }
}

fn ews_recurrence_xml(event: &AccessibleEvent) -> String {
    let Some(recurrence) = rrule_to_ews_recurrence(&event.recurrence_rule, &event.date) else {
        return String::new();
    };
    recurrence
}

fn rrule_to_ews_recurrence(rrule: &str, start_date: &str) -> Option<String> {
    let fields = rrule_fields(rrule);
    let freq = fields.get("FREQ")?.as_str();
    let interval = fields
        .get("INTERVAL")
        .cloned()
        .unwrap_or_else(|| "1".to_string());
    let pattern = match freq {
        "DAILY" => format!(
            "<t:DailyRecurrence><t:Interval>{}</t:Interval></t:DailyRecurrence>",
            escape_xml(&interval)
        ),
        "WEEKLY" => {
            let days = fields
                .get("BYDAY")
                .map(|value| {
                    value
                        .split(',')
                        .filter_map(rrule_weekday_to_ews)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "Monday".to_string());
            format!(
                concat!(
                    "<t:WeeklyRecurrence>",
                    "<t:Interval>{interval}</t:Interval>",
                    "<t:DaysOfWeek>{days}</t:DaysOfWeek>",
                    "</t:WeeklyRecurrence>"
                ),
                interval = escape_xml(&interval),
                days = escape_xml(&days),
            )
        }
        "MONTHLY" => {
            let day = fields.get("BYMONTHDAY")?;
            format!(
                concat!(
                    "<t:AbsoluteMonthlyRecurrence>",
                    "<t:Interval>{interval}</t:Interval>",
                    "<t:DayOfMonth>{day}</t:DayOfMonth>",
                    "</t:AbsoluteMonthlyRecurrence>"
                ),
                interval = escape_xml(&interval),
                day = escape_xml(day),
            )
        }
        "YEARLY" => {
            let day = fields.get("BYMONTHDAY")?;
            let month = fields.get("BYMONTH").and_then(|value| {
                value
                    .parse::<u32>()
                    .ok()
                    .and_then(rrule_month_number_to_ews)
            })?;
            format!(
                concat!(
                    "<t:AbsoluteYearlyRecurrence>",
                    "<t:DayOfMonth>{day}</t:DayOfMonth>",
                    "<t:Month>{month}</t:Month>",
                    "</t:AbsoluteYearlyRecurrence>"
                ),
                day = escape_xml(day),
                month = month,
            )
        }
        _ => return None,
    };
    let range = if let Some(count) = fields.get("COUNT") {
        format!(
            concat!(
                "<t:NumberedRecurrence>",
                "<t:StartDate>{}</t:StartDate>",
                "<t:NumberOfOccurrences>{}</t:NumberOfOccurrences>",
                "</t:NumberedRecurrence>"
            ),
            escape_xml(start_date),
            escape_xml(count),
        )
    } else if let Some(until) = fields.get("UNTIL") {
        format!(
            concat!(
                "<t:EndDateRecurrence>",
                "<t:StartDate>{}</t:StartDate>",
                "<t:EndDate>{}</t:EndDate>",
                "</t:EndDateRecurrence>"
            ),
            escape_xml(start_date),
            escape_xml(&rrule_until_to_ews_date(until)),
        )
    } else {
        format!(
            "<t:NoEndRecurrence><t:StartDate>{}</t:StartDate></t:NoEndRecurrence>",
            escape_xml(start_date)
        )
    };
    Some(format!("<t:Recurrence>{pattern}{range}</t:Recurrence>"))
}

fn rrule_fields(rrule: &str) -> HashMap<String, String> {
    rrule
        .split(';')
        .filter_map(|part| part.split_once('='))
        .map(|(key, value)| (key.trim().to_ascii_uppercase(), value.trim().to_string()))
        .collect()
}

fn rrule_weekday_to_ews(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_uppercase().as_str() {
        "MO" => Some("Monday"),
        "TU" => Some("Tuesday"),
        "WE" => Some("Wednesday"),
        "TH" => Some("Thursday"),
        "FR" => Some("Friday"),
        "SA" => Some("Saturday"),
        "SU" => Some("Sunday"),
        _ => None,
    }
}

fn rrule_month_number_to_ews(value: u32) -> Option<&'static str> {
    match value {
        1 => Some("January"),
        2 => Some("February"),
        3 => Some("March"),
        4 => Some("April"),
        5 => Some("May"),
        6 => Some("June"),
        7 => Some("July"),
        8 => Some("August"),
        9 => Some("September"),
        10 => Some("October"),
        11 => Some("November"),
        12 => Some("December"),
        _ => None,
    }
}

fn rrule_until_to_ews_date(value: &str) -> String {
    let date = value.split('T').next().unwrap_or(value);
    if date.len() == 8 {
        format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8])
    } else {
        date.to_string()
    }
}

fn task_item_summary_xml(task: &ClientTask) -> String {
    let change_key = task_change_key(task, None);
    task_item_summary_xml_with_change_key(task, &change_key)
}

fn task_item_summary_xml_with_change_key(task: &ClientTask, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Task>",
            "<t:ItemId Id=\"task:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Status>{status}</t:Status>",
            "{due_date}",
            "{complete_date}",
            "</t:Task>"
        ),
        id = task.id,
        change_key = escape_xml(change_key),
        title = escape_xml(&task.title),
        status = ews_task_status(&task.status),
        due_date = optional_text_element("t:DueDate", task.due_at.as_deref()),
        complete_date = optional_text_element("t:CompleteDate", task.completed_at.as_deref()),
    )
}

fn task_item_xml(task: &ClientTask) -> String {
    let change_key = task_change_key(task, None);
    task_item_xml_with_change_key(task, &change_key)
}

fn task_item_xml_with_change_key(task: &ClientTask, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Task>",
            "<t:ItemId Id=\"task:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Body BodyType=\"Text\">{description}</t:Body>",
            "<t:Status>{status}</t:Status>",
            "{due_date}",
            "{complete_date}",
            "</t:Task>"
        ),
        id = task.id,
        change_key = escape_xml(change_key),
        folder_id = task.task_list_id,
        title = escape_xml(&task.title),
        description = escape_xml(&task.description),
        status = ews_task_status(&task.status),
        due_date = optional_text_element("t:DueDate", task.due_at.as_deref()),
        complete_date = optional_text_element("t:CompleteDate", task.completed_at.as_deref()),
    )
}

fn ews_task_status(status: &str) -> &'static str {
    match status {
        "in-progress" => "InProgress",
        "completed" => "Completed",
        "cancelled" => "Deferred",
        _ => "NotStarted",
    }
}

fn optional_text_element(name: &str, value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("<{name}>{}</{name}>", escape_xml(value)))
        .unwrap_or_default()
}

fn ews_datetime(date: &str, time: &str) -> String {
    format!("{}T{}:00Z", date.trim(), time.trim())
}

fn event_end_datetime(event: &AccessibleEvent) -> String {
    let (hour, minute) = event
        .time
        .split_once(':')
        .and_then(|(hour, minute)| Some((hour.parse::<i32>().ok()?, minute.parse::<i32>().ok()?)))
        .unwrap_or((0, 0));
    let total = hour
        .saturating_mul(60)
        .saturating_add(minute)
        .saturating_add(event.duration_minutes.max(0));
    let end_hour = (total / 60).min(23);
    let end_minute = total % 60;
    ews_datetime(&event.date, &format!("{end_hour:02}:{end_minute:02}"))
}

fn first_name(name: &str) -> String {
    name.split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

fn last_name(name: &str) -> String {
    name.split_whitespace()
        .skip(1)
        .collect::<Vec<_>>()
        .join(" ")
}

fn count_tag_occurrences(value: &str, needle: &str) -> usize {
    value.match_indices(needle).count()
}

fn soap_response(body: String) -> Response {
    let envelope = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" ",
            "xmlns:m=\"http://schemas.microsoft.com/exchange/services/2006/messages\" ",
            "xmlns:t=\"http://schemas.microsoft.com/exchange/services/2006/types\">",
            "<s:Header><t:ServerVersionInfo MajorVersion=\"15\" MinorVersion=\"0\" MajorBuildNumber=\"0\" MinorBuildNumber=\"0\" Version=\"Exchange2013\"/></s:Header>",
            "<s:Body>{body}</s:Body>",
            "</s:Envelope>"
        ),
        body = body,
    );
    xml_response(StatusCode::OK, envelope)
}

fn is_rpc_proxy_echo_request(method: &Method, headers: &HeaderMap) -> bool {
    let method = method.as_str();
    if method != "RPC_IN_DATA" && method != "RPC_OUT_DATA" {
        return false;
    }

    is_rpc_proxy_msrpc_request(headers)
}

pub(crate) fn is_rpc_proxy_in_data_channel_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> bool {
    method.as_str() == "RPC_IN_DATA"
        && is_rpc_proxy_endpoint_ping(uri)
        && is_rpc_proxy_msrpc_request(headers)
}

fn is_rpc_proxy_endpoint_ping(uri: &Uri) -> bool {
    uri.query().is_some_and(is_rpc_proxy_endpoint_query)
}

fn is_rpc_proxy_endpoint_query(query: &str) -> bool {
    query.contains(":6001") || query.contains(":6002") || query.contains(":6004")
}

fn is_rpc_proxy_msrpc_request(headers: &HeaderMap) -> bool {
    let user_agent = mapi::safe_header(headers, "user-agent")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let accept = mapi::safe_header(headers, "accept")
        .unwrap_or_default()
        .to_ascii_lowercase();
    user_agent == "msrpc" || accept.contains("application/rpc")
}

#[derive(Debug, Clone, Copy)]
struct RpcProxyOutDataConnect {
    receive_window_size: u32,
}

fn parse_rpc_proxy_out_data_connect_request(
    method: &Method,
    headers: &HeaderMap,
    request_body: &[u8],
) -> Option<RpcProxyOutDataConnect> {
    if method.as_str() != "RPC_OUT_DATA"
        || request_body.is_empty()
        || !is_rpc_proxy_msrpc_request(headers)
    {
        return None;
    }
    parse_rpc_proxy_conn_a1_rts_pdu(request_body)
}

fn parse_rpc_proxy_conn_a1_rts_pdu(body: &[u8]) -> Option<RpcProxyOutDataConnect> {
    if body.len() < 20 || body.get(0..4) != Some(&[0x05, 0x00, 0x14, 0x03]) {
        return None;
    }
    let fragment_length = u16::from_le_bytes([body[8], body[9]]) as usize;
    let flags = u16::from_le_bytes([body[16], body[17]]);
    let command_count = u16::from_le_bytes([body[18], body[19]]);
    if fragment_length != body.len() || flags != 0 || command_count != 4 {
        return None;
    }

    let mut offset = 20;
    let version = parse_rpc_rts_u32_command(body, &mut offset, 6)?;
    if version == 0 {
        return None;
    }
    parse_rpc_rts_cookie_command(body, &mut offset, 3)?;
    parse_rpc_rts_cookie_command(body, &mut offset, 3)?;
    let receive_window_size = parse_rpc_rts_u32_command(body, &mut offset, 0)?;
    (offset == body.len()).then_some(RpcProxyOutDataConnect {
        receive_window_size,
    })
}

fn parse_rpc_rts_u32_command(
    body: &[u8],
    offset: &mut usize,
    expected_command: u32,
) -> Option<u32> {
    let command = read_le_u32(body, *offset)?;
    let value = read_le_u32(body, *offset + 4)?;
    if command != expected_command {
        return None;
    }
    *offset += 8;
    Some(value)
}

fn parse_rpc_rts_cookie_command(
    body: &[u8],
    offset: &mut usize,
    expected_command: u32,
) -> Option<[u8; 16]> {
    let command = read_le_u32(body, *offset)?;
    let cookie = body.get(*offset + 4..*offset + 20)?;
    if command != expected_command {
        return None;
    }
    let mut result = [0u8; 16];
    result.copy_from_slice(cookie);
    *offset += 20;
    Some(result)
}

fn read_le_u32(body: &[u8], offset: usize) -> Option<u32> {
    let bytes = body.get(offset..offset + 4)?;
    Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn rpc_proxy_rts_connect_body(client_receive_window_size: u32) -> Vec<u8> {
    let receive_window_size = client_receive_window_size.clamp(1, RPC_PROXY_RECEIVE_WINDOW_SIZE);
    let mut body = rpc_proxy_connection_timeout_pdu();
    body.extend_from_slice(&rpc_proxy_connection_established_pdu(receive_window_size));
    body
}

fn rpc_proxy_connection_timeout_pdu() -> Vec<u8> {
    let mut body = rpc_proxy_rts_header(0, 1, 28);
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&RPC_PROXY_CONNECTION_TIMEOUT_MS.to_le_bytes());
    body
}

fn rpc_proxy_connection_established_pdu(receive_window_size: u32) -> Vec<u8> {
    let mut body = rpc_proxy_rts_header(0, 3, 44);
    body.extend_from_slice(&6u32.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&receive_window_size.to_le_bytes());
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&RPC_PROXY_CONNECTION_TIMEOUT_MS.to_le_bytes());
    body
}

fn rpc_proxy_rts_header(flags: u16, command_count: u16, fragment_length: u16) -> Vec<u8> {
    let mut body = Vec::with_capacity(fragment_length as usize);
    body.extend_from_slice(&[0x05, 0x00, 0x14, 0x03, 0x10, 0x00, 0x00, 0x00]);
    body.extend_from_slice(&fragment_length.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&flags.to_le_bytes());
    body.extend_from_slice(&command_count.to_le_bytes());
    body
}

fn rpc_proxy_rts_connect_response(client_receive_window_size: u32) -> Response {
    rpc_proxy_binary_response(
        rpc_proxy_rts_connect_body(client_receive_window_size),
        RPC_PROXY_RTS_CONNECT_STATUS,
    )
}

fn rpc_proxy_mailstore_ping_response(uri: &Uri, client_receive_window_size: u32) -> Response {
    let mut body = rpc_proxy_rts_connect_body(client_receive_window_size);
    if let Some(query) = uri
        .query()
        .filter(|query| is_rpc_proxy_endpoint_query(query))
    {
        body.extend_from_slice(&rpc_proxy_endpoint_ping_bind_ack_body(uri, 1));
        mark_rpc_proxy_out_endpoint_bind_ack(query);
    }
    rpc_proxy_mailstore_held_open_response(uri, body)
}

pub(crate) fn mark_rpc_proxy_out_endpoint_bind_ack(query: &str) {
    let mut pending = rpc_proxy_out_endpoint_bind_acks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let count = pending.entry(query.to_string()).or_insert(0);
    *count = count.saturating_add(1);
}

fn consume_rpc_proxy_out_endpoint_bind_ack(query: &str) -> bool {
    let mut pending = rpc_proxy_out_endpoint_bind_acks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(count) = pending.get_mut(query) else {
        return false;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        pending.remove(query);
    }
    true
}

fn rpc_proxy_out_endpoint_bind_acks() -> &'static Mutex<HashMap<String, usize>> {
    static BIND_ACKS: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    BIND_ACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rpc_proxy_endpoint_ping_bind_ack_body(uri: &Uri, call_id: u32) -> Vec<u8> {
    let result_count = uri
        .query()
        .filter(|query| query.contains(":6004"))
        .map(|_| 2)
        .unwrap_or(1);
    rpc_proxy_dce_bind_ack_body_with_result_count(call_id, result_count)
}

fn rpc_proxy_dce_bind_ack_body(call_id: u32, request: &[u8]) -> Vec<u8> {
    let result_count = rpc_proxy_dce_bind_context_count(request).unwrap_or(1);
    rpc_proxy_dce_bind_ack_body_with_result_count(call_id, result_count)
}

fn rpc_proxy_dce_bind_ack_body_with_result_count(call_id: u32, result_count: u8) -> Vec<u8> {
    const DCE_RPC_BIND_ACK: u8 = 0x0c;
    rpc_proxy_dce_context_ack_body(call_id, DCE_RPC_BIND_ACK, result_count)
}

fn rpc_proxy_dce_alter_context_response_body(call_id: u32, request: &[u8]) -> Vec<u8> {
    const DCE_RPC_ALTER_CONTEXT_RESPONSE: u8 = 0x0f;
    let result_count = rpc_proxy_dce_bind_context_count(request).unwrap_or(1);
    rpc_proxy_dce_context_ack_body(call_id, DCE_RPC_ALTER_CONTEXT_RESPONSE, result_count)
}

fn rpc_proxy_dce_bind_context_count(request: &[u8]) -> Option<u8> {
    let count = *request.get(24)?;
    (count > 0).then_some(count)
}

fn rpc_proxy_dce_fault_response(call_id: u32, status: u32) -> Vec<u8> {
    const DCE_RPC_FAULT: u8 = 0x03;
    const DCE_RPC_FIRST_FRAG: u8 = 0x01;
    const DCE_RPC_LAST_FRAG: u8 = 0x02;
    const FRAGMENT_LENGTH: u16 = 32;

    let mut packet = Vec::with_capacity(FRAGMENT_LENGTH as usize);
    packet.extend_from_slice(&[
        0x05,
        0x00,
        DCE_RPC_FAULT,
        DCE_RPC_FIRST_FRAG | DCE_RPC_LAST_FRAG,
        0x10,
        0x00,
        0x00,
        0x00,
    ]);
    packet.extend_from_slice(&FRAGMENT_LENGTH.to_le_bytes());
    packet.extend_from_slice(&0u16.to_le_bytes());
    packet.extend_from_slice(&call_id.to_le_bytes());
    packet.extend_from_slice(&0u32.to_le_bytes());
    packet.extend_from_slice(&0u16.to_le_bytes());
    packet.push(0);
    packet.push(0);
    packet.extend_from_slice(&status.to_le_bytes());
    packet.extend_from_slice(&0u32.to_le_bytes());
    packet
}

fn rpc_proxy_dce_context_ack_body(call_id: u32, packet_type: u8, result_count: u8) -> Vec<u8> {
    const DCE_RPC_FIRST_FRAG: u8 = 0x01;
    const DCE_RPC_LAST_FRAG: u8 = 0x02;
    const DCE_RPC_MAX_FRAG: u16 = 5840;
    const DCE_RPC_NDR_TRANSFER_SYNTAX: [u8; 20] = [
        0x04, 0x5d, 0x88, 0x8a, 0xeb, 0x1c, 0xc9, 0x11, 0x9f, 0xe8, 0x08, 0x00, 0x2b, 0x10, 0x48,
        0x60, 0x02, 0x00, 0x00, 0x00,
    ];
    let mut body = Vec::new();
    body.extend_from_slice(&DCE_RPC_MAX_FRAG.to_le_bytes());
    body.extend_from_slice(&DCE_RPC_MAX_FRAG.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&1u16.to_le_bytes());
    body.push(0);
    body.push(0);
    body.push(result_count);
    body.push(0);
    body.extend_from_slice(&0u16.to_le_bytes());
    for result_index in 0..result_count {
        if result_index == 0 {
            body.extend_from_slice(&0u16.to_le_bytes());
            body.extend_from_slice(&0u16.to_le_bytes());
            body.extend_from_slice(&DCE_RPC_NDR_TRANSFER_SYNTAX);
        } else {
            body.extend_from_slice(&2u16.to_le_bytes());
            body.extend_from_slice(&2u16.to_le_bytes());
            body.extend_from_slice(&[0u8; 20]);
        }
    }

    let verifier = ntlm::connect_level_challenge_verifier();
    body.push(verifier.auth_type);
    body.push(verifier.auth_level);
    body.push(0);
    body.push(0);
    body.extend_from_slice(&verifier.context_id.to_le_bytes());
    body.extend_from_slice(&verifier.value);

    let fragment_length = (16 + body.len()) as u16;
    let mut packet = Vec::with_capacity(fragment_length as usize);
    packet.extend_from_slice(&[
        0x05,
        0x00,
        packet_type,
        DCE_RPC_FIRST_FRAG | DCE_RPC_LAST_FRAG,
        0x10,
        0x00,
        0x00,
        0x00,
    ]);
    packet.extend_from_slice(&fragment_length.to_le_bytes());
    packet.extend_from_slice(&(verifier.value.len() as u16).to_le_bytes());
    packet.extend_from_slice(&call_id.to_le_bytes());
    packet.extend_from_slice(&body);
    packet
}

fn rpc_proxy_echo_response() -> Response {
    rpc_proxy_binary_response(RPC_PROXY_ECHO_BODY.to_vec(), RPC_PROXY_ECHO_STATUS)
}

fn rpc_proxy_in_channel_response(uri: &Uri) -> Response {
    if should_hold_rpc_proxy_in_channel(uri) {
        return rpc_proxy_held_open_binary_response(
            Vec::new(),
            RPC_PROXY_IN_CHANNEL_STATUS,
            rpc_proxy_channel_hold_ms(),
            false,
            true,
        );
    }

    let mut response = StatusCode::OK.into_response();
    response
        .headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
    decorate_rpc_proxy_binary_response(
        &mut response,
        0,
        String::new(),
        RPC_PROXY_IN_CHANNEL_STATUS,
    );
    response
}

fn rpc_proxy_mailstore_held_open_response(uri: &Uri, body: Vec<u8>) -> Response {
    let Some(query) = uri.query() else {
        return rpc_proxy_binary_response(body, RPC_PROXY_ENDPOINT_PING_STATUS);
    };
    let hold_open_ms = rpc_proxy_channel_hold_ms();
    if hold_open_ms == 0 {
        return rpc_proxy_binary_response(body, RPC_PROXY_ENDPOINT_PING_STATUS);
    }

    let payload_bytes = body.len();
    let payload_preview_hex = mapi::debug_payload_preview_hex(&body);
    let query = query.to_string();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    register_rpc_proxy_out_channel(&query, sender);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(hold_open_ms)).await;
        remove_rpc_proxy_out_channel(&query);
    });

    let initial = Some(Ok::<Bytes, std::io::Error>(Bytes::from(body)));
    let followups = tokio_stream::wrappers::UnboundedReceiverStream::new(receiver).map(Ok);
    let stream = tokio_stream::iter(initial).chain(followups);
    let mut response = Response::new(Body::from_stream(stream));
    decorate_rpc_proxy_binary_response(
        &mut response,
        payload_bytes,
        payload_preview_hex,
        RPC_PROXY_ENDPOINT_PING_STATUS,
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&RPC_PROXY_OUT_CHANNEL_CONTENT_LENGTH.to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("131072")),
    );
    response
}

fn register_rpc_proxy_out_channel(query: &str, sender: tokio::sync::mpsc::UnboundedSender<Bytes>) {
    let mut channels = rpc_proxy_out_channels()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    channels.insert(query.to_string(), sender);
}

fn send_rpc_proxy_out_channel(query: &str, bytes: Vec<u8>) -> bool {
    let sender = {
        let channels = rpc_proxy_out_channels()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        channels.get(query).cloned()
    };
    if let Some(sender) = sender {
        return sender.send(Bytes::from(bytes)).is_ok();
    }
    false
}

fn remove_rpc_proxy_out_channel(query: &str) {
    let mut channels = rpc_proxy_out_channels()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    channels.remove(query);
}

fn rpc_proxy_out_channels(
) -> &'static Mutex<HashMap<String, tokio::sync::mpsc::UnboundedSender<Bytes>>> {
    static CHANNELS: OnceLock<Mutex<HashMap<String, tokio::sync::mpsc::UnboundedSender<Bytes>>>> =
        OnceLock::new();
    CHANNELS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn should_hold_rpc_proxy_in_channel(uri: &Uri) -> bool {
    let Some(query) = uri
        .query()
        .filter(|query| is_rpc_proxy_endpoint_query(query))
    else {
        return false;
    };
    let hold_open_ms = rpc_proxy_channel_hold_ms();
    if hold_open_ms == 0 {
        return false;
    }
    if query.contains(":6002") || query.contains(":6004") {
        return true;
    }

    let now = Instant::now();
    let mut channels = rpc_proxy_in_channel_openers()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    channels.retain(|_, first_seen| {
        now.duration_since(*first_seen) <= Duration::from_millis(hold_open_ms)
    });
    channels.insert(query.to_string(), now).is_some()
}

fn rpc_proxy_in_channel_openers() -> &'static Mutex<HashMap<String, Instant>> {
    static OPENERS: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    OPENERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rpc_proxy_binary_response(body: Vec<u8>, status: &'static str) -> Response {
    if (status == RPC_PROXY_RTS_CONNECT_STATUS || status == RPC_PROXY_ENDPOINT_PING_STATUS)
        && rpc_proxy_channel_hold_ms() > 0
    {
        return rpc_proxy_held_open_binary_response(
            body,
            status,
            rpc_proxy_channel_hold_ms(),
            true,
            true,
        );
    }

    let payload_bytes = body.len();
    let payload_preview_hex = mapi::debug_payload_preview_hex(&body);
    let mut response = (StatusCode::OK, body).into_response();
    decorate_rpc_proxy_binary_response(&mut response, payload_bytes, payload_preview_hex, status);
    response
}

fn rpc_proxy_held_open_binary_response(
    body: Vec<u8>,
    status: &'static str,
    hold_open_ms: u64,
    send_initial_body: bool,
    include_content_length: bool,
) -> Response {
    let payload_bytes = body.len();
    let payload_preview_hex = mapi::debug_payload_preview_hex(&body);
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    tokio::spawn(async move {
        if send_initial_body {
            let _ = sender.send(Ok(Bytes::from(body))).await;
        }
        tokio::time::sleep(Duration::from_millis(hold_open_ms)).await;
    });

    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    decorate_rpc_proxy_binary_response(&mut response, payload_bytes, payload_preview_hex, status);
    if include_content_length {
        response.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&RPC_PROXY_OUT_CHANNEL_CONTENT_LENGTH.to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("131072")),
        );
    }
    response
}

fn decorate_rpc_proxy_binary_response(
    response: &mut Response,
    payload_bytes: usize,
    payload_preview_hex: String,
    status: &'static str,
) {
    response
        .extensions_mut()
        .insert(RpcProxyResponseDebug { payload_bytes });
    if !payload_preview_hex.is_empty() {
        response
            .extensions_mut()
            .insert(RpcProxyResponsePayloadPreview {
                hex: payload_preview_hex,
            });
    }
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/rpc"));
    response
        .headers_mut()
        .insert(CONNECTION, HeaderValue::from_static("Keep-Alive"));
    response
        .headers_mut()
        .insert(RPC_PROXY_COMPAT_STATUS, HeaderValue::from_static(status));
}

#[derive(Clone, Copy, Debug)]
struct RpcProxyResponseDebug {
    payload_bytes: usize,
}

#[derive(Clone, Debug)]
struct RpcProxyResponsePayloadPreview {
    hex: String,
}

fn rpc_proxy_response_payload_bytes(response: &Response) -> Option<usize> {
    response
        .extensions()
        .get::<RpcProxyResponseDebug>()
        .map(|debug| debug.payload_bytes)
}

fn rpc_proxy_response_payload_preview_hex(response: &Response) -> Option<&str> {
    response
        .extensions()
        .get::<RpcProxyResponsePayloadPreview>()
        .map(|preview| preview.hex.as_str())
}

fn spawn_rpc_proxy_in_data_drain<S, V>(
    store: S,
    validator: Validator<V>,
    principal: AccountPrincipal,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Body,
) where
    S: ExchangeStore + Send + Sync + 'static,
    V: Detector + Send + Sync + 'static,
{
    let method = method.to_string();
    let path = uri.path().to_string();
    let query = uri.query().unwrap_or_default().to_string();
    let trace_id = mapi::safe_header(headers, "x-trace-id").unwrap_or_default();
    let client_request_id = mapi::safe_header(headers, "client-request-id").unwrap_or_default();
    let x_request_id = mapi::safe_header(headers, "x-requestid").unwrap_or_default();
    let user_agent = mapi::safe_header(headers, "user-agent").unwrap_or_default();

    tokio::spawn(async move {
        let started_at = Instant::now();
        info!(
            rca_debug = true,
            adapter = "rpcproxy",
            method = %method,
            path = %path,
            query = %query,
            response_kind = "in-channel-open",
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            http_status = 200u16,
            request_body_bytes = 0usize,
            response_payload_bytes = 0usize,
            request_body_preview_hex = "",
            response_payload_preview_hex = "",
            duration_ms = 0.0f64,
            user_agent = %user_agent,
            message = "rca debug rpc proxy in data stream opened"
        );

        let mut stream = body.into_data_stream();
        let mut pdu_buffer = Vec::new();
        let mut total_body_bytes = 0usize;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    total_body_bytes += bytes.len();
                    let request_body_preview_hex = mapi::debug_payload_preview_hex(bytes.as_ref());
                    pdu_buffer.extend_from_slice(bytes.as_ref());
                    while let Some(response) =
                        rpc_proxy_in_channel_response_for_endpoint_query_with_store(
                            &store,
                            &validator,
                            &principal,
                            &query,
                            &mut pdu_buffer,
                        )
                        .await
                    {
                        let response_payload_bytes = response.len();
                        let response_payload_preview_hex =
                            mapi::debug_payload_preview_hex(&response);
                        let forwarded = send_rpc_proxy_out_channel(&query, response);
                        info!(
                            rca_debug = true,
                            adapter = "rpcproxy",
                            method = %method,
                            path = %path,
                            query = %query,
                            response_kind = if forwarded {
                                "out-channel-forwarded"
                            } else {
                                "out-channel-missing"
                            },
                            trace_id = %trace_id,
                            client_request_id = %client_request_id,
                            x_request_id = %x_request_id,
                            http_status = 200u16,
                            response_payload_bytes = response_payload_bytes,
                            response_payload_preview_hex = %response_payload_preview_hex,
                            duration_ms = started_at.elapsed().as_secs_f64() * 1000.0,
                            user_agent = %user_agent,
                            message = "rca debug rpc proxy forwarded response from in data stream"
                        );
                    }
                    info!(
                        rca_debug = true,
                        adapter = "rpcproxy",
                        method = %method,
                        path = %path,
                        query = %query,
                        response_kind = "in-channel-data",
                        trace_id = %trace_id,
                        client_request_id = %client_request_id,
                        x_request_id = %x_request_id,
                        http_status = 200u16,
                        request_body_bytes = bytes.len(),
                        total_request_body_bytes = total_body_bytes,
                        request_body_preview_hex = %request_body_preview_hex,
                        duration_ms = started_at.elapsed().as_secs_f64() * 1000.0,
                        user_agent = %user_agent,
                        message = "rca debug rpc proxy in data chunk"
                    );
                }
                Err(error) => {
                    warn!(
                        rca_debug = true,
                        adapter = "rpcproxy",
                        method = %method,
                        path = %path,
                        query = %query,
                        response_kind = "in-channel-error",
                        trace_id = %trace_id,
                        client_request_id = %client_request_id,
                        x_request_id = %x_request_id,
                        http_status = 200u16,
                        total_request_body_bytes = total_body_bytes,
                        duration_ms = started_at.elapsed().as_secs_f64() * 1000.0,
                        user_agent = %user_agent,
                        error = %error,
                        message = "rca debug rpc proxy in data stream error"
                    );
                    return;
                }
            }
        }

        info!(
            rca_debug = true,
            adapter = "rpcproxy",
            method = %method,
            path = %path,
            query = %query,
            response_kind = "in-channel-finished",
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            http_status = 200u16,
            total_request_body_bytes = total_body_bytes,
            duration_ms = started_at.elapsed().as_secs_f64() * 1000.0,
            user_agent = %user_agent,
            message = "rca debug rpc proxy in data stream finished"
        );
    });
}

#[cfg(test)]
pub(crate) fn rpc_proxy_in_channel_response_for_buffer(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    rpc_proxy_in_channel_response_for_endpoint_query("", buffer)
}

#[cfg(test)]
pub(crate) fn rpc_proxy_in_channel_response_for_endpoint_query(
    endpoint_query: &str,
    buffer: &mut Vec<u8>,
) -> Option<Vec<u8>> {
    let mut offset = 0usize;
    while offset + 16 <= buffer.len() {
        if buffer.get(offset..offset + 2) != Some(&[0x05, 0x00]) {
            offset += 1;
            continue;
        }

        let fragment_length = u16::from_le_bytes([buffer[offset + 8], buffer[offset + 9]]) as usize;
        if fragment_length < 16 {
            offset += 1;
            continue;
        }

        let fragment_end = offset + fragment_length;
        if fragment_end > buffer.len() {
            if offset > 0 {
                buffer.drain(..offset);
            }
            return None;
        }

        let response =
            rpc_proxy_endpoint_response_for_fragment(endpoint_query, &buffer[offset..fragment_end]);
        if let Some(response) = response {
            buffer.drain(..fragment_end);
            return Some(response);
        }

        offset = fragment_end;
    }
    if offset > 0 {
        buffer.drain(..offset);
    }
    None
}

pub(crate) async fn rpc_proxy_in_channel_response_for_endpoint_query_with_store<S, V>(
    store: &S,
    validator: &Validator<V>,
    principal: &AccountPrincipal,
    endpoint_query: &str,
    buffer: &mut Vec<u8>,
) -> Option<Vec<u8>>
where
    S: ExchangeStore,
    V: Detector,
{
    let mut offset = 0usize;
    while offset + 16 <= buffer.len() {
        if buffer.get(offset..offset + 2) != Some(&[0x05, 0x00]) {
            offset += 1;
            continue;
        }

        let fragment_length = u16::from_le_bytes([buffer[offset + 8], buffer[offset + 9]]) as usize;
        if fragment_length < 16 {
            offset += 1;
            continue;
        }

        let fragment_end = offset + fragment_length;
        if fragment_end > buffer.len() {
            if offset > 0 {
                buffer.drain(..offset);
            }
            return None;
        }

        let response = rpc_proxy_endpoint_response_for_fragment_with_store(
            store,
            validator,
            principal,
            endpoint_query,
            &buffer[offset..fragment_end],
        )
        .await;
        if let Some(response) = response {
            buffer.drain(..fragment_end);
            return Some(response);
        }

        offset = fragment_end;
    }
    if offset > 0 {
        buffer.drain(..offset);
    }
    None
}

#[cfg(test)]
fn rpc_proxy_endpoint_response_for_fragment(endpoint_query: &str, bytes: &[u8]) -> Option<Vec<u8>> {
    if bytes.get(0..2) != Some(&[0x05, 0x00]) {
        return None;
    }
    let fragment_length = u16::from_le_bytes([*bytes.get(8)?, *bytes.get(9)?]) as usize;
    if fragment_length > bytes.len() || fragment_length < 16 {
        return None;
    }
    let call_id = read_le_u32(bytes, 12)?;
    match bytes.get(2).copied()? {
        0x0b => {
            if consume_rpc_proxy_out_endpoint_bind_ack(endpoint_query) {
                return None;
            }
            return Some(rpc_proxy_dce_bind_ack_body(call_id, bytes));
        }
        0x0e => return Some(rpc_proxy_dce_alter_context_response_body(call_id, bytes)),
        0x00 => {}
        _ => return None,
    }
    if fragment_length < 24 {
        return None;
    }
    let alloc_hint = read_le_u32(bytes, 16)?;
    let context_id = u16::from_le_bytes([*bytes.get(20)?, *bytes.get(21)?]);
    let opnum = u16::from_le_bytes([*bytes.get(22)?, *bytes.get(23)?]);
    if endpoint_query.contains(":6002") {
        match opnum {
            0 if alloc_hint >= 4 => {
                return Some(rpc_proxy_rfri_get_new_dsa_response(call_id, endpoint_query));
            }
            1 if alloc_hint >= 8 => {
                return Some(rpc_proxy_rfri_get_fqdn_response(call_id, endpoint_query));
            }
            _ => {}
        }
    }
    if endpoint_query.contains(":6001") {
        match opnum {
            1 if alloc_hint >= 20 => return Some(rpc_proxy_emsmdb_disconnect_response(call_id)),
            10 if alloc_hint >= 20 => return Some(rpc_proxy_emsmdb_connect_ex_response(call_id)),
            11 if alloc_hint >= 20 => return Some(rpc_proxy_emsmdb_rpc_ext2_response(call_id)),
            _ => {}
        }
    }
    match (context_id, opnum) {
        (0, 1) if alloc_hint == 4 => {
            let requested_stats = read_le_u32(bytes, 24)?;
            Some(rpc_proxy_mgmt_inq_stats_response(call_id, requested_stats))
        }
        (2, 0) if alloc_hint >= 44 => Some(rpc_proxy_nspi_bind_response(call_id)),
        (2, 1) if alloc_hint >= 20 => Some(rpc_proxy_nspi_unbind_response(call_id)),
        (2, 2) if alloc_hint >= 20 => Some(rpc_proxy_nspi_update_stat_response(call_id)),
        (2, 3) if alloc_hint >= 20 => Some(rpc_proxy_nspi_query_rows_response(call_id, bytes)),
        (2, 4) if alloc_hint >= 20 => Some(rpc_proxy_nspi_query_rows_response(call_id, bytes)),
        (2, 5) if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_matches_response(call_id, bytes)),
        (2, 6) if alloc_hint >= 20 => Some(rpc_proxy_nspi_resort_restriction_response(call_id)),
        (2, 7) if alloc_hint >= 20 => Some(rpc_proxy_nspi_minimal_ids_response(call_id)),
        (2, 8) if alloc_hint >= 16 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        (2, 9) if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_props_response(call_id, bytes)),
        (2, 10) if alloc_hint >= 20 => Some(rpc_proxy_nspi_compare_mids_response(call_id)),
        (2, 12) if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_special_table_response(call_id)),
        (2, 13) if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_props_response(call_id, bytes)),
        (2, 16) if alloc_hint >= 12 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        (2, 17) if alloc_hint >= 12 => {
            Some(rpc_proxy_nspi_get_names_from_ids_response(call_id, bytes))
        }
        (2, 18) if alloc_hint >= 20 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        (2, 19) if alloc_hint >= 24 => Some(rpc_proxy_nspi_resolve_names_response(call_id, bytes)),
        (2, 20) if alloc_hint >= 24 => Some(rpc_proxy_nspi_resolve_names_response(call_id, bytes)),
        _ => None,
    }
}

async fn rpc_proxy_endpoint_response_for_fragment_with_store<S, V>(
    store: &S,
    validator: &Validator<V>,
    principal: &AccountPrincipal,
    endpoint_query: &str,
    bytes: &[u8],
) -> Option<Vec<u8>>
where
    S: ExchangeStore,
    V: Detector,
{
    if bytes.get(0..2) != Some(&[0x05, 0x00]) {
        return None;
    }
    let fragment_length = u16::from_le_bytes([*bytes.get(8)?, *bytes.get(9)?]) as usize;
    if fragment_length > bytes.len() || fragment_length < 16 {
        return None;
    }
    let call_id = read_le_u32(bytes, 12)?;
    match bytes.get(2).copied()? {
        0x0b => {
            if consume_rpc_proxy_out_endpoint_bind_ack(endpoint_query) {
                return None;
            }
            return Some(rpc_proxy_dce_bind_ack_body(call_id, bytes));
        }
        0x0e => return Some(rpc_proxy_dce_alter_context_response_body(call_id, bytes)),
        0x00 => {}
        _ => return None,
    }
    if fragment_length < 24 {
        return None;
    }
    let alloc_hint = read_le_u32(bytes, 16)?;
    let context_id = u16::from_le_bytes([*bytes.get(20)?, *bytes.get(21)?]);
    let opnum = u16::from_le_bytes([*bytes.get(22)?, *bytes.get(23)?]);
    if endpoint_query.contains(":6002") {
        match opnum {
            0 if alloc_hint >= 4 => {
                return Some(rpc_proxy_rfri_get_new_dsa_response_for_principal(
                    call_id,
                    endpoint_query,
                    principal,
                ));
            }
            1 if alloc_hint >= 8 => {
                return Some(rpc_proxy_rfri_get_fqdn_response_for_principal(
                    call_id,
                    endpoint_query,
                    principal,
                ));
            }
            _ => {}
        }
    }
    if endpoint_query.contains(":6001") {
        match opnum {
            1 if alloc_hint >= 20 => return Some(rpc_proxy_emsmdb_disconnect_response(call_id)),
            10 if alloc_hint >= 20 => {
                return Some(rpc_proxy_emsmdb_connect_ex_response_for_principal(
                    call_id, principal,
                ));
            }
            11 if alloc_hint >= 20 => {
                return Some(
                    rpc_proxy_emsmdb_rpc_ext2_response_for_principal(
                        store, validator, principal, call_id, bytes,
                    )
                    .await,
                );
            }
            _ => {}
        }
    }
    match (context_id, opnum) {
        (0, 1) if alloc_hint == 4 => {
            let requested_stats = read_le_u32(bytes, 24)?;
            Some(rpc_proxy_mgmt_inq_stats_response(call_id, requested_stats))
        }
        (2, 0) if alloc_hint >= 44 => Some(rpc_proxy_nspi_bind_response(call_id)),
        (2, 1) if alloc_hint >= 20 => Some(rpc_proxy_nspi_unbind_response(call_id)),
        (2, 2) if alloc_hint >= 20 => Some(rpc_proxy_nspi_update_stat_response(call_id)),
        (2, 3) if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_query_rows_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        (2, 4) if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_query_rows_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        (2, 5) if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_get_matches_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        (2, 6) if alloc_hint >= 20 => Some(rpc_proxy_nspi_resort_restriction_response(call_id)),
        (2, 7) if alloc_hint >= 20 => Some(rpc_proxy_nspi_minimal_ids_response(call_id)),
        (2, 8) if alloc_hint >= 16 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        (2, 9) if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_get_props_response_for_principal(store, call_id, bytes, principal).await,
        ),
        (2, 10) if alloc_hint >= 20 => Some(rpc_proxy_nspi_compare_mids_response(call_id)),
        (2, 12) if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_special_table_response(call_id)),
        (2, 13) if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_get_props_response_for_principal(store, call_id, bytes, principal).await,
        ),
        (2, 16) if alloc_hint >= 12 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        (2, 17) if alloc_hint >= 12 => {
            Some(rpc_proxy_nspi_get_names_from_ids_response(call_id, bytes))
        }
        (2, 18) if alloc_hint >= 20 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        (2, 19) if alloc_hint >= 24 => Some(
            rpc_proxy_nspi_resolve_names_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        (2, 20) if alloc_hint >= 24 => Some(
            rpc_proxy_nspi_resolve_names_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        _ => None,
    }
}

fn rpc_proxy_mgmt_inq_stats_response(call_id: u32, requested_stats: u32) -> Vec<u8> {
    let stat_count = requested_stats.min(4);
    let stats = [1u32, 0u32, 1u32, 1u32];
    let mut stub = Vec::with_capacity(8 + (stat_count as usize * 4) + 4);
    stub.extend_from_slice(&stat_count.to_le_bytes());
    stub.extend_from_slice(&stat_count.to_le_bytes());
    for value in stats.iter().take(stat_count as usize) {
        stub.extend_from_slice(&value.to_le_bytes());
    }
    stub.extend_from_slice(&0u32.to_le_bytes());

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_emsmdb_connect_ex_response(call_id: u32) -> Vec<u8> {
    let mut context = [0u8; 20];
    context[4..20].copy_from_slice(Uuid::nil().as_bytes());
    rpc_proxy_emsmdb_connect_ex_response_with_context(call_id, &context)
}

fn rpc_proxy_emsmdb_connect_ex_response_for_principal(
    call_id: u32,
    principal: &AccountPrincipal,
) -> Vec<u8> {
    let context = mapi::create_rpc_emsmdb_context(principal);
    rpc_proxy_emsmdb_connect_ex_response_with_context(call_id, &context)
}

fn rpc_proxy_emsmdb_connect_ex_response_with_context(call_id: u32, context: &[u8; 20]) -> Vec<u8> {
    let mut stub = Vec::new();
    rpc_proxy_push_emsmdb_context_handle(&mut stub, context);
    push_le_u32(&mut stub, 60_000);
    push_le_u32(&mut stub, 6);
    push_le_u32(&mut stub, 10_000);
    stub.extend_from_slice(&0x0304u16.to_le_bytes());
    while stub.len() % 4 != 0 {
        stub.push(0);
    }
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);
    for value in [15u16, 0x263c, 0] {
        stub.extend_from_slice(&value.to_le_bytes());
    }
    for value in [12u16, 0x183e, 1000] {
        stub.extend_from_slice(&value.to_le_bytes());
    }
    while stub.len() % 4 != 0 {
        stub.push(0);
    }
    push_le_u32(&mut stub, 1);
    rpc_proxy_push_ndr_byte_array(&mut stub, &[]);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_emsmdb_rpc_ext2_response(call_id: u32) -> Vec<u8> {
    let mut context = [0u8; 20];
    context[4..20].copy_from_slice(Uuid::nil().as_bytes());
    rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer(call_id, &context, Vec::new())
}

async fn rpc_proxy_emsmdb_rpc_ext2_response_for_principal<S, V>(
    store: &S,
    validator: &Validator<V>,
    principal: &AccountPrincipal,
    call_id: u32,
    request: &[u8],
) -> Vec<u8>
where
    S: ExchangeStore,
    V: Detector,
{
    let (context, rop_buffer) = match rpc_proxy_emsmdb_rpc_ext2_request(request) {
        Ok(request) => request,
        Err(error) => {
            warn!(
                rca_debug = true,
                adapter = "rpcproxy",
                mailbox = %principal.email,
                error = %error,
                message = "rpc proxy emsmdb request parsing failed"
            );
            return rpc_proxy_dce_fault_response(call_id, RPC_PROXY_DCE_FAULT_PROTOCOL_ERROR);
        }
    };
    let rop_buffer =
        match mapi::execute_rpc_emsmdb_rops(store, validator, principal, &context, &rop_buffer)
            .await
        {
            Ok(rop_buffer) => rop_buffer,
            Err(error) => {
                warn!(
                    rca_debug = true,
                    adapter = "rpcproxy",
                    mailbox = %principal.email,
                    error = %error,
                    message = "rpc proxy emsmdb execution failed"
                );
                return rpc_proxy_dce_fault_response(call_id, RPC_PROXY_DCE_FAULT_PROTOCOL_ERROR);
            }
        };
    rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer(call_id, &context, rop_buffer)
}

fn rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer(
    call_id: u32,
    context: &[u8; 20],
    rop_buffer: Vec<u8>,
) -> Vec<u8> {
    let rgb_out = if rop_buffer.is_empty() {
        rpc_proxy_rpc_header_ext_payload(&[])
    } else {
        rop_buffer
    };
    let mut stub = Vec::new();
    rpc_proxy_push_emsmdb_context_handle(&mut stub, context);
    push_le_u32(&mut stub, 0);
    rpc_proxy_push_ndr_byte_array(&mut stub, &rgb_out);
    push_le_u32(&mut stub, rgb_out.len() as u32);
    rpc_proxy_push_ndr_byte_array(&mut stub, &[]);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_emsmdb_disconnect_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::new();
    stub.extend_from_slice(&[0; 20]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_push_emsmdb_context_handle(stub: &mut Vec<u8>, context: &[u8; 20]) {
    stub.extend_from_slice(context);
}

fn rpc_proxy_push_ndr_byte_array(stub: &mut Vec<u8>, value: &[u8]) {
    push_le_u32(stub, value.len() as u32);
    push_le_u32(stub, 0);
    push_le_u32(stub, value.len() as u32);
    stub.extend_from_slice(value);
    while stub.len() % 4 != 0 {
        stub.push(0);
    }
}

fn rpc_proxy_rpc_header_ext_payload(payload: &[u8]) -> Vec<u8> {
    let size = payload.len().min(u16::MAX as usize) as u16;
    let mut buffer = Vec::with_capacity(8 + payload.len());
    buffer.extend_from_slice(&0u16.to_le_bytes());
    buffer.extend_from_slice(&0x0004u16.to_le_bytes());
    buffer.extend_from_slice(&size.to_le_bytes());
    buffer.extend_from_slice(&size.to_le_bytes());
    buffer.extend_from_slice(payload);
    buffer
}

fn rpc_proxy_emsmdb_rpc_ext2_request(request: &[u8]) -> Result<([u8; 20], Vec<u8>)> {
    let fragment_length = request
        .get(8..10)
        .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]) as usize)
        .ok_or_else(|| anyhow!("truncated DCE/RPC request header"))?;
    let stub = request
        .get(24..fragment_length)
        .ok_or_else(|| anyhow!("truncated EcDoRpcExt2 request stub"))?;
    let context: [u8; 20] = stub
        .get(0..20)
        .ok_or_else(|| anyhow!("missing EcDoRpcExt2 context handle"))?
        .try_into()
        .map_err(|_| anyhow!("invalid EcDoRpcExt2 context handle"))?;
    for offset in 20..stub.len().saturating_sub(8) {
        let candidate = &stub[offset..];
        if candidate.get(0..2) != Some(&[0, 0]) {
            continue;
        }
        let flags = u16::from_le_bytes([candidate[2], candidate[3]]);
        let size = u16::from_le_bytes([candidate[4], candidate[5]]) as usize;
        let size_actual = u16::from_le_bytes([candidate[6], candidate[7]]) as usize;
        if flags & !0x0004 != 0 || size == 0 || size > size_actual {
            continue;
        }
        let end = 8 + size;
        let Some(payload) = candidate.get(8..end) else {
            continue;
        };
        let Some(rop_size_bytes) = payload.get(0..2) else {
            continue;
        };
        let rop_size = u16::from_le_bytes(
            rop_size_bytes
                .try_into()
                .map_err(|_| anyhow!("invalid ROP buffer size"))?,
        ) as usize;
        if rop_size >= 2 && payload.len() >= rop_size {
            return Ok((context, candidate[..end].to_vec()));
        }
    }
    Err(anyhow!(
        "missing valid EcDoRpcExt2 RPC_HEADER_EXT ROP payload"
    ))
}

#[cfg(test)]
fn rpc_proxy_rfri_get_new_dsa_response(call_id: u32, endpoint_query: &str) -> Vec<u8> {
    let server = rpc_proxy_referral_server_name(endpoint_query);
    let mut stub = Vec::with_capacity(40 + server.len());
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0x0002_0000);
    push_le_u32(&mut stub, 0x0002_0004);
    rpc_proxy_push_ndr_ascii_string(&mut stub, &server);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_rfri_get_new_dsa_response_for_principal(
    call_id: u32,
    endpoint_query: &str,
    principal: &AccountPrincipal,
) -> Vec<u8> {
    let server = rpc_proxy_referral_server_name_for_principal(endpoint_query, principal);
    let mut stub = Vec::with_capacity(40 + server.len());
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0x0002_0000);
    push_le_u32(&mut stub, 0x0002_0004);
    rpc_proxy_push_ndr_ascii_string(&mut stub, &server);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_rfri_get_fqdn_response(call_id: u32, endpoint_query: &str) -> Vec<u8> {
    let server = rpc_proxy_referral_server_name(endpoint_query);
    let mut stub = Vec::with_capacity(32 + server.len());
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_ndr_ascii_string(&mut stub, &server);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_rfri_get_fqdn_response_for_principal(
    call_id: u32,
    endpoint_query: &str,
    principal: &AccountPrincipal,
) -> Vec<u8> {
    let server = rpc_proxy_referral_server_name_for_principal(endpoint_query, principal);
    let mut stub = Vec::with_capacity(32 + server.len());
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_ndr_ascii_string(&mut stub, &server);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_referral_server_name(endpoint_query: &str) -> String {
    endpoint_query
        .split_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty())
        .unwrap_or("localhost")
        .to_ascii_lowercase()
}

fn rpc_proxy_referral_server_name_for_principal(
    endpoint_query: &str,
    principal: &AccountPrincipal,
) -> String {
    endpoint_query
        .split_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| {
            let domain = principal
                .email
                .split_once('@')
                .map(|(_, domain)| domain)
                .filter(|domain| !domain.is_empty())
                .unwrap_or("localhost");
            format!("mail.{domain}").to_ascii_lowercase()
        })
}

fn rpc_proxy_nspi_bind_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(28);
    stub.extend_from_slice(&0u32.to_le_bytes());
    stub.extend_from_slice(&0u32.to_le_bytes());
    stub.extend_from_slice(&[
        0x4c, 0x50, 0x45, 0x00, 0x4e, 0x53, 0x50, 0x49, 0x43, 0x54, 0x58, 0x00, 0x00, 0x00, 0x00,
        0x01,
    ]);
    stub.extend_from_slice(&0u32.to_le_bytes());

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_unbind_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(24);
    for _ in 0..5 {
        push_le_u32(&mut stub, 0);
    }
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_update_stat_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(44);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 2);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 0x04e4);
    push_le_u32(&mut stub, 0x0409);
    push_le_u32(&mut stub, 0x0409);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_nspi_query_rows_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let row_values = rpc_proxy_nspi_row_values(request, &tags);
    let mut stub = Vec::with_capacity(256);
    rpc_proxy_push_stat(&mut stub);
    rpc_proxy_push_rowset_pointer(&mut stub, &[row_values]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

async fn rpc_proxy_nspi_query_rows_response_for_principal<S>(
    store: &S,
    call_id: u32,
    request: &[u8],
    principal: &AccountPrincipal,
) -> Vec<u8>
where
    S: ExchangeStore,
{
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let entries = rpc_proxy_address_book_entries(store, principal).await;
    let rows = rpc_proxy_filter_nspi_entries(&entries, request)
        .into_iter()
        .map(|entry| rpc_proxy_nspi_row_values_for_entry(&tags, entry))
        .collect::<Vec<_>>();
    let mut stub = Vec::with_capacity(256);
    rpc_proxy_push_stat(&mut stub);
    rpc_proxy_push_rowset_pointer(&mut stub, &rows);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_nspi_get_matches_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let row_values = rpc_proxy_nspi_row_values(request, &tags);
    let mut stub = Vec::with_capacity(280);
    rpc_proxy_push_stat(&mut stub);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[2]);
    rpc_proxy_push_rowset_pointer(&mut stub, &[row_values]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

async fn rpc_proxy_nspi_get_matches_response_for_principal<S>(
    store: &S,
    call_id: u32,
    request: &[u8],
    principal: &AccountPrincipal,
) -> Vec<u8>
where
    S: ExchangeStore,
{
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let entries = rpc_proxy_address_book_entries(store, principal).await;
    let matched = rpc_proxy_filter_nspi_entries(&entries, request);
    let rows = matched
        .iter()
        .map(|entry| rpc_proxy_nspi_row_values_for_entry(&tags, entry))
        .collect::<Vec<_>>();
    let mids = matched
        .iter()
        .map(|entry| rpc_proxy_nspi_entry_id(entry))
        .collect::<Vec<_>>();
    let mut stub = Vec::with_capacity(280);
    rpc_proxy_push_stat(&mut stub);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &mids);
    rpc_proxy_push_rowset_pointer(&mut stub, &rows);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}
fn rpc_proxy_nspi_resort_restriction_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(68);
    rpc_proxy_push_stat(&mut stub);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[2]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_minimal_ids_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(32);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[2]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_property_tags_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(80);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, RPC_PROXY_NSPI_BOOTSTRAP_PROPERTY_TAGS);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_get_names_from_ids_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    let tags = rpc_proxy_nspi_known_property_tags(request);
    let mut stub = Vec::with_capacity(24 + tags.len() * 12);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0x0002_0000);
    push_le_u32(&mut stub, tags.len() as u32);
    push_le_u32(&mut stub, tags.len() as u32);
    for tag in tags {
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, tag);
    }
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_nspi_get_props_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let row_values = rpc_proxy_nspi_row_values(request, &tags);
    let mut stub = Vec::with_capacity(192);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_row(&mut stub, &row_values);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

async fn rpc_proxy_nspi_get_props_response_for_principal<S>(
    store: &S,
    call_id: u32,
    request: &[u8],
    principal: &AccountPrincipal,
) -> Vec<u8>
where
    S: ExchangeStore,
{
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let entries = rpc_proxy_address_book_entries(store, principal).await;
    let row_values = rpc_proxy_requested_nspi_entry(&entries, request)
        .or_else(|| {
            entries
                .iter()
                .find(|entry| rpc_proxy_nspi_entry_is_principal(entry, principal))
        })
        .map(|entry| rpc_proxy_nspi_row_values_for_entry(&tags, entry))
        .unwrap_or_default();
    let mut stub = Vec::with_capacity(192);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_row(&mut stub, &row_values);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_compare_mids_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(8);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_get_special_table_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(220);
    push_le_u32(&mut stub, 1);
    let row = vec![
        (0x3001_001f, RpcProxyNspiValue::String("Global Address List".to_string())),
        (0x0ffe_0003, RpcProxyNspiValue::U32(2)),
        (0x3000_0003, RpcProxyNspiValue::U32(1)),
        (
            0x3002_001f,
            RpcProxyNspiValue::String(
                "/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Address Lists/cn=Global Address List".to_string(),
            ),
        ),
    ];
    rpc_proxy_push_rowset_pointer(&mut stub, &[row]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_nspi_resolve_names_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    const MID_RESOLVED: u32 = 2;
    const PR_DISPLAY_NAME_A: u32 = 0x3001_001e;
    const PR_EMAIL_ADDRESS_A: u32 = 0x3003_001e;

    let smtp_address = rpc_proxy_nspi_requested_smtp_address(request)
        .unwrap_or_else(|| "unknown@localhost".to_string());
    let display_name = rpc_proxy_display_name_for_smtp_address(&smtp_address);
    let property_tags = rpc_proxy_nspi_requested_resolve_property_tags(request);
    let row_values: Vec<(u32, String)> = property_tags
        .into_iter()
        .filter_map(|tag| match tag {
            PR_EMAIL_ADDRESS_A => Some((tag, smtp_address.clone())),
            PR_DISPLAY_NAME_A => Some((tag, display_name.clone())),
            _ => None,
        })
        .collect();
    let row_values = if row_values.is_empty() {
        vec![
            (PR_EMAIL_ADDRESS_A, smtp_address),
            (PR_DISPLAY_NAME_A, display_name),
        ]
    } else {
        row_values
    };

    let mut stub = Vec::with_capacity(192);
    let mut deferred_strings = Vec::new();

    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[MID_RESOLVED]);

    push_le_u32(&mut stub, 0x0002_0004);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, row_values.len() as u32);
    push_le_u32(&mut stub, 0x0002_0008);
    push_le_u32(&mut stub, row_values.len() as u32);

    for (index, (property_tag, value)) in row_values.iter().enumerate() {
        push_le_u32(&mut stub, *property_tag);
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, property_tag & 0xffff);
        push_le_u32(&mut stub, 0x0002_000c + (index as u32 * 4));
        rpc_proxy_push_ndr_ascii_string(&mut deferred_strings, value);
    }
    stub.extend_from_slice(&deferred_strings);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

async fn rpc_proxy_nspi_resolve_names_response_for_principal<S>(
    store: &S,
    call_id: u32,
    request: &[u8],
    principal: &AccountPrincipal,
) -> Vec<u8>
where
    S: ExchangeStore,
{
    const MID_RESOLVED: u32 = 2;
    const PR_DISPLAY_NAME_A: u32 = 0x3001_001e;
    const PR_EMAIL_ADDRESS_A: u32 = 0x3003_001e;

    let entries = rpc_proxy_address_book_entries(store, principal).await;
    let principal_entry = rpc_proxy_principal_address_book_entry(principal);
    let lookup_values = rpc_proxy_nspi_lookup_values(request);
    let matched = lookup_values
        .first()
        .and_then(|value| rpc_proxy_match_nspi_entry(&entries, value))
        .or_else(|| {
            lookup_values
                .iter()
                .any(|value| rpc_proxy_nspi_principal_matches(value, principal))
                .then_some(&principal_entry)
        });
    let Some(entry) = matched else {
        let mut stub = Vec::with_capacity(64);
        push_le_u32(&mut stub, 0x0002_0000);
        rpc_proxy_push_property_tag_array(&mut stub, &[0]);
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, 0);
        return rpc_proxy_dce_response(call_id, &stub);
    };
    let smtp_address = entry.email.to_ascii_lowercase();
    let display_name = entry.display_name.clone();
    let property_tags = rpc_proxy_nspi_requested_resolve_property_tags(request);
    let row_values: Vec<(u32, String)> = property_tags
        .into_iter()
        .filter_map(|tag| match tag {
            PR_EMAIL_ADDRESS_A => Some((tag, smtp_address.clone())),
            PR_DISPLAY_NAME_A => Some((tag, display_name.clone())),
            _ => None,
        })
        .collect();
    let row_values = if row_values.is_empty() {
        vec![
            (PR_EMAIL_ADDRESS_A, smtp_address),
            (PR_DISPLAY_NAME_A, display_name),
        ]
    } else {
        row_values
    };

    let mut stub = Vec::with_capacity(192);
    let mut deferred_strings = Vec::new();

    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[MID_RESOLVED]);

    push_le_u32(&mut stub, 0x0002_0004);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, row_values.len() as u32);
    push_le_u32(&mut stub, 0x0002_0008);
    push_le_u32(&mut stub, row_values.len() as u32);

    for (index, (property_tag, value)) in row_values.iter().enumerate() {
        push_le_u32(&mut stub, *property_tag);
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, property_tag & 0xffff);
        push_le_u32(&mut stub, 0x0002_000c + (index as u32 * 4));
        rpc_proxy_push_ndr_ascii_string(&mut deferred_strings, value);
    }
    stub.extend_from_slice(&deferred_strings);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

const RPC_PROXY_NSPI_BOOTSTRAP_PROPERTY_TAGS: &[u32] = &[
    0x3001_001f,
    0x39fe_001f,
    0x3003_001f,
    0x3a00_001f,
    0x0ffe_0003,
    0x3000_0003,
    0x3004_001f,
    0x3002_001f,
    0x3005_001f,
];

enum RpcProxyNspiValue {
    String(String),
    U32(u32),
}

fn rpc_proxy_nspi_requested_property_tags(request: &[u8]) -> Vec<u32> {
    let mut tags = rpc_proxy_nspi_known_property_tags(request);
    if tags.is_empty() {
        tags.extend_from_slice(RPC_PROXY_NSPI_BOOTSTRAP_PROPERTY_TAGS);
    }
    tags
}

fn rpc_proxy_nspi_known_property_tags(request: &[u8]) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut offset = 24usize;
    while offset + 4 <= request.len() {
        let Some(tag) = read_le_u32(request, offset) else {
            break;
        };
        if RPC_PROXY_NSPI_BOOTSTRAP_PROPERTY_TAGS.contains(&tag) && !tags.contains(&tag) {
            tags.push(tag);
        }
        offset += 4;
    }
    tags
}

fn rpc_proxy_nspi_requested_resolve_property_tags(request: &[u8]) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut offset = 24usize;
    while offset + 4 <= request.len() {
        let Some(tag) = read_le_u32(request, offset) else {
            break;
        };
        if matches!(tag, 0x3001_001e | 0x3003_001e) && !tags.contains(&tag) {
            tags.push(tag);
        }
        offset += 4;
    }
    tags
}

fn rpc_proxy_nspi_requested_smtp_address(request: &[u8]) -> Option<String> {
    const SMTP_PREFIX_UTF16LE: &[u8] = b"=\0S\0M\0T\0P\0:\0";
    const SMTP_PREFIX_ASCII: &[u8] = b"=SMTP:";

    if let Some(start) = request.windows(SMTP_PREFIX_ASCII.len()).position(|window| {
        window
            .iter()
            .zip(SMTP_PREFIX_ASCII)
            .all(|(actual, expected)| actual.eq_ignore_ascii_case(expected))
    }) {
        let mut end = start + SMTP_PREFIX_ASCII.len();
        while end < request.len() && request[end] != 0 {
            end += 1;
        }
        if let Ok(value) = std::str::from_utf8(&request[start + SMTP_PREFIX_ASCII.len()..end]) {
            let value = value.trim().to_ascii_lowercase();
            if value.contains('@') {
                return Some(value);
            }
        }
    }

    let start = request
        .windows(SMTP_PREFIX_UTF16LE.len())
        .position(|window| {
            window
                .chunks_exact(2)
                .zip(SMTP_PREFIX_UTF16LE.chunks_exact(2))
                .all(|(actual, expected)| {
                    actual[0].eq_ignore_ascii_case(&expected[0]) && actual[1] == expected[1]
                })
        })?;
    let mut units = Vec::new();
    let mut offset = start + SMTP_PREFIX_UTF16LE.len();
    while offset + 1 < request.len() {
        let unit = u16::from_le_bytes([request[offset], request[offset + 1]]);
        if unit == 0 {
            break;
        }
        units.push(unit);
        offset += 2;
    }
    String::from_utf16(&units)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| value.contains('@'))
}

#[cfg(test)]
fn rpc_proxy_display_name_for_smtp_address(address: &str) -> String {
    let local_part = address.split('@').next().unwrap_or(address).trim();
    let mut chars = local_part.chars();
    let Some(first) = chars.next() else {
        return address.to_string();
    };
    let mut display_name = first.to_uppercase().collect::<String>();
    display_name.push_str(chars.as_str());
    display_name
}

fn rpc_proxy_push_property_tag_array(buffer: &mut Vec<u8>, values: &[u32]) {
    push_le_u32(buffer, values.len() as u32 + 1);
    push_le_u32(buffer, values.len() as u32);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, values.len() as u32);
    for value in values {
        push_le_u32(buffer, *value);
    }
}

fn rpc_proxy_push_stat(buffer: &mut Vec<u8>) {
    push_le_u32(buffer, 0);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, 2);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, 1);
    push_le_u32(buffer, 1);
    push_le_u32(buffer, 0x04e4);
    push_le_u32(buffer, 0x0409);
    push_le_u32(buffer, 0x0409);
}

#[cfg(test)]
fn rpc_proxy_nspi_row_values(request: &[u8], tags: &[u32]) -> Vec<(u32, RpcProxyNspiValue)> {
    let smtp_address = rpc_proxy_nspi_requested_smtp_address(request)
        .unwrap_or_else(|| "unknown@localhost".to_string());
    let display_name = rpc_proxy_display_name_for_smtp_address(&smtp_address);
    tags.iter()
        .map(|tag| {
            let value = match *tag {
                0x3001_001f | 0x3a00_001f => RpcProxyNspiValue::String(display_name.clone()),
                0x39fe_001f | 0x3003_001f | 0x3004_001f => {
                    RpcProxyNspiValue::String(smtp_address.clone())
                }
                0x3002_001f => RpcProxyNspiValue::String("SMTP".to_string()),
                0x3005_001f => RpcProxyNspiValue::String(format!(
                    "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}",
                    smtp_address.replace('@', "-").replace('.', "-")
                )),
                0x0ffe_0003 => RpcProxyNspiValue::U32(6),
                0x3000_0003 => RpcProxyNspiValue::U32(2),
                _ if *tag & 0xffff == 0x0003 => RpcProxyNspiValue::U32(0),
                _ => RpcProxyNspiValue::String(String::new()),
            };
            (*tag, value)
        })
        .collect()
}

async fn rpc_proxy_address_book_entries<S>(
    store: &S,
    principal: &AccountPrincipal,
) -> Vec<ExchangeAddressBookEntry>
where
    S: ExchangeStore,
{
    match store.fetch_address_book_entries(principal.account_id).await {
        Ok(entries) => entries,
        Err(_) => Vec::new(),
    }
}

fn rpc_proxy_principal_address_book_entry(
    principal: &AccountPrincipal,
) -> ExchangeAddressBookEntry {
    ExchangeAddressBookEntry {
        id: principal.account_id,
        display_name: principal.display_name.clone(),
        email: principal.email.clone(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
    }
}

fn rpc_proxy_nspi_row_values_for_entry(
    tags: &[u32],
    entry: &ExchangeAddressBookEntry,
) -> Vec<(u32, RpcProxyNspiValue)> {
    let smtp_address = entry.email.to_ascii_lowercase();
    let display_name = entry.display_name.clone();
    tags.iter()
        .map(|tag| {
            let value = match *tag {
                0x3001_001f | 0x3a00_001f => RpcProxyNspiValue::String(display_name.clone()),
                0x39fe_001f | 0x3003_001f | 0x3004_001f => {
                    RpcProxyNspiValue::String(smtp_address.clone())
                }
                0x3002_001f => RpcProxyNspiValue::String("SMTP".to_string()),
                0x3005_001f => RpcProxyNspiValue::String(format!(
                    "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}",
                    rpc_proxy_nspi_entry_legacy_name(entry)
                )),
                0x0ffe_0003 => RpcProxyNspiValue::U32(6),
                0x3000_0003 => RpcProxyNspiValue::U32(rpc_proxy_nspi_entry_id(entry)),
                _ if *tag & 0xffff == 0x0003 => RpcProxyNspiValue::U32(0),
                _ => RpcProxyNspiValue::String(String::new()),
            };
            (*tag, value)
        })
        .collect()
}

fn rpc_proxy_nspi_entry_id(entry: &ExchangeAddressBookEntry) -> u32 {
    let bytes = entry.id.as_bytes();
    let value = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => value | 0x8000_0000,
        ExchangeAddressBookEntryKind::Contact => value | 0x4000_0000,
    }
    .max(2)
}

fn rpc_proxy_nspi_entry_legacy_name(entry: &ExchangeAddressBookEntry) -> String {
    let prefix = match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => "acct",
        ExchangeAddressBookEntryKind::Contact => "contact",
    };
    let source = if entry.email.trim().is_empty() {
        entry.id.to_string()
    } else {
        entry.email.clone()
    };
    let legacy_user = source
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("{prefix}-{legacy_user}")
}

fn rpc_proxy_filter_nspi_entries<'a>(
    entries: &'a [ExchangeAddressBookEntry],
    request: &[u8],
) -> Vec<&'a ExchangeAddressBookEntry> {
    let values = rpc_proxy_nspi_lookup_values(request);
    if values.is_empty() {
        return entries.iter().collect();
    }
    entries
        .iter()
        .filter(|entry| {
            values
                .iter()
                .any(|value| rpc_proxy_nspi_entry_matches(entry, value))
        })
        .collect()
}

fn rpc_proxy_requested_nspi_entry<'a>(
    entries: &'a [ExchangeAddressBookEntry],
    request: &[u8],
) -> Option<&'a ExchangeAddressBookEntry> {
    rpc_proxy_nspi_requested_mids(request)
        .iter()
        .find_map(|mid| {
            entries
                .iter()
                .find(|entry| rpc_proxy_nspi_entry_id(entry) == *mid)
        })
        .or_else(|| {
            rpc_proxy_nspi_lookup_values(request)
                .iter()
                .find_map(|value| rpc_proxy_match_nspi_entry(entries, value))
        })
}

fn rpc_proxy_match_nspi_entry<'a>(
    entries: &'a [ExchangeAddressBookEntry],
    value: &str,
) -> Option<&'a ExchangeAddressBookEntry> {
    entries
        .iter()
        .find(|entry| {
            rpc_proxy_nspi_entry_matches(entry, value)
                && rpc_proxy_nspi_entry_exact_match(entry, value)
        })
        .or_else(|| {
            entries
                .iter()
                .find(|entry| rpc_proxy_nspi_entry_matches(entry, value))
        })
}

fn rpc_proxy_nspi_entry_is_principal(
    entry: &ExchangeAddressBookEntry,
    principal: &AccountPrincipal,
) -> bool {
    entry.entry_kind == ExchangeAddressBookEntryKind::Account && entry.id == principal.account_id
}

fn rpc_proxy_nspi_principal_matches(value: &str, principal: &AccountPrincipal) -> bool {
    let value = rpc_proxy_normalize_nspi_lookup_value(value);
    let email = principal.email.to_ascii_lowercase();
    let display_name = principal.display_name.to_ascii_lowercase();
    value == email
        || value == display_name
        || value == format!("smtp:{email}")
        || value == format!("=smtp:{email}")
        || email.contains(value.as_str())
}

fn rpc_proxy_nspi_entry_exact_match(entry: &ExchangeAddressBookEntry, value: &str) -> bool {
    let value = rpc_proxy_normalize_nspi_lookup_value(value);
    let email = entry.email.to_ascii_lowercase();
    value == email
        || value == entry.display_name.to_ascii_lowercase()
        || value
            == format!(
                "/o=lpe/ou=exchange administrative group/cn=recipients/cn={}",
                rpc_proxy_nspi_entry_legacy_name(entry)
            )
}

fn rpc_proxy_nspi_entry_matches(entry: &ExchangeAddressBookEntry, value: &str) -> bool {
    let value = rpc_proxy_normalize_nspi_lookup_value(value);
    if value.is_empty() {
        return false;
    }
    rpc_proxy_nspi_entry_exact_match(entry, &value)
        || entry.email.to_ascii_lowercase().contains(value.as_str())
        || entry
            .display_name
            .to_ascii_lowercase()
            .contains(value.as_str())
}

fn rpc_proxy_nspi_requested_mids(request: &[u8]) -> Vec<u32> {
    let mut mids = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= request.len() {
        if let Some(value) = read_le_u32(request, offset) {
            if value >= 2 && !mids.contains(&value) {
                mids.push(value);
            }
        }
        offset += 4;
    }
    mids
}

fn rpc_proxy_nspi_lookup_values(request: &[u8]) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(address) = rpc_proxy_nspi_requested_smtp_address(request) {
        values.push(address);
    }
    values.extend(rpc_proxy_nspi_ascii_lookup_values(request));
    values.extend(rpc_proxy_nspi_utf16_lookup_values(request));
    values.sort();
    values.dedup();
    values
}

fn rpc_proxy_nspi_ascii_lookup_values(request: &[u8]) -> Vec<String> {
    request
        .split(|byte| *byte == 0)
        .filter_map(|bytes| {
            if bytes.len() < 3 {
                return None;
            }
            let value = String::from_utf8_lossy(bytes);
            let value = rpc_proxy_normalize_nspi_lookup_value(&value);
            (!value.is_empty() && (value.contains('@') || value.contains("/cn="))).then_some(value)
        })
        .collect()
}

fn rpc_proxy_nspi_utf16_lookup_values(request: &[u8]) -> Vec<String> {
    let mut values = Vec::new();
    let mut start = 0usize;
    while start + 3 < request.len() {
        let mut units = Vec::new();
        let mut offset = start;
        while offset + 1 < request.len() {
            let unit = u16::from_le_bytes([request[offset], request[offset + 1]]);
            if unit == 0 {
                break;
            }
            if unit < 0x20 && !matches!(unit, 0x09 | 0x0a | 0x0d) {
                units.clear();
                break;
            }
            units.push(unit);
            offset += 2;
        }
        if units.len() >= 3 {
            if let Ok(value) = String::from_utf16(&units) {
                let value = rpc_proxy_normalize_nspi_lookup_value(&value);
                if !value.is_empty() && (value.contains('@') || value.contains("/cn=")) {
                    values.push(value);
                }
            }
        }
        start += 1;
    }
    values
}

fn rpc_proxy_normalize_nspi_lookup_value(value: &str) -> String {
    let mut value = value.trim().trim_matches('\0').to_ascii_lowercase();
    if let Some(rest) = value.strip_prefix("=smtp:") {
        value = rest.to_string();
    } else if let Some(rest) = value.strip_prefix("smtp:") {
        value = rest.to_string();
    }
    value
}

fn rpc_proxy_push_rowset_pointer(buffer: &mut Vec<u8>, rows: &[Vec<(u32, RpcProxyNspiValue)>]) {
    push_le_u32(buffer, 0x0002_0004);
    push_le_u32(buffer, rows.len() as u32);
    push_le_u32(buffer, rows.len() as u32);
    for row in rows {
        rpc_proxy_push_property_row(buffer, row);
    }
}

fn rpc_proxy_push_property_row(buffer: &mut Vec<u8>, row_values: &[(u32, RpcProxyNspiValue)]) {
    let mut deferred = Vec::new();
    push_le_u32(buffer, 0);
    push_le_u32(buffer, row_values.len() as u32);
    push_le_u32(buffer, 0x0002_0008);
    push_le_u32(buffer, row_values.len() as u32);
    for (index, (property_tag, value)) in row_values.iter().enumerate() {
        push_le_u32(buffer, *property_tag);
        push_le_u32(buffer, 0);
        push_le_u32(buffer, property_tag & 0xffff);
        match value {
            RpcProxyNspiValue::U32(value) => push_le_u32(buffer, *value),
            RpcProxyNspiValue::String(value) if property_tag & 0xffff == 0x001f => {
                push_le_u32(buffer, 0x0002_000c + (index as u32 * 4));
                rpc_proxy_push_ndr_utf16_string(&mut deferred, value);
            }
            RpcProxyNspiValue::String(value) => {
                push_le_u32(buffer, 0x0002_000c + (index as u32 * 4));
                rpc_proxy_push_ndr_ascii_string(&mut deferred, value);
            }
        }
    }
    buffer.extend_from_slice(&deferred);
}

fn rpc_proxy_push_ndr_ascii_string(buffer: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    let count = bytes.len() as u32 + 1;
    push_le_u32(buffer, count);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, count);
    buffer.extend_from_slice(bytes);
    buffer.push(0);
    while buffer.len() % 4 != 0 {
        buffer.push(0);
    }
}

fn rpc_proxy_push_ndr_utf16_string(buffer: &mut Vec<u8>, value: &str) {
    let units: Vec<u16> = value.encode_utf16().chain(std::iter::once(0)).collect();
    push_le_u32(buffer, units.len() as u32);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, units.len() as u32);
    for unit in units {
        buffer.extend_from_slice(&unit.to_le_bytes());
    }
    while buffer.len() % 4 != 0 {
        buffer.push(0);
    }
}

fn push_le_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn rpc_proxy_dce_response(call_id: u32, stub: &[u8]) -> Vec<u8> {
    const RESPONSE_BODY_HEADER_LENGTH: usize = 8;
    let fragment_length = 16 + RESPONSE_BODY_HEADER_LENGTH + stub.len();
    let mut packet = Vec::with_capacity(fragment_length);
    packet.extend_from_slice(&[0x05, 0x00, 0x02, 0x03, 0x10, 0x00, 0x00, 0x00]);
    packet.extend_from_slice(&(fragment_length as u16).to_le_bytes());
    packet.extend_from_slice(&0u16.to_le_bytes());
    packet.extend_from_slice(&call_id.to_le_bytes());
    packet.extend_from_slice(&(stub.len() as u32).to_le_bytes());
    packet.extend_from_slice(&0u16.to_le_bytes());
    packet.push(0);
    packet.push(0);
    packet.extend_from_slice(stub);
    packet
}

#[cfg(not(test))]
fn rpc_proxy_channel_hold_ms() -> u64 {
    std::env::var("LPE_RPC_PROXY_OUT_CHANNEL_HOLD_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(u64::from(RPC_PROXY_CONNECTION_TIMEOUT_MS))
        .min(14_400_000)
}

#[cfg(test)]
fn rpc_proxy_channel_hold_ms() -> u64 {
    1
}

fn rpc_proxy_accepted_response(principal: &AccountPrincipal) -> Response {
    let mut response = (
        StatusCode::OK,
        format!(
            "LPE RPC proxy compatibility authentication accepted for {}. Use MAPI over HTTP for mailbox access.\n",
            principal.email
        ),
    )
        .into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        RPC_PROXY_COMPAT_STATUS,
        HeaderValue::from_static("auth-accepted"),
    );
    response
}

fn rpc_proxy_auth_challenge_response(message: &str) -> Response {
    let mut response = (
        StatusCode::UNAUTHORIZED,
        format!("LPE RPC proxy authentication required: {message}\n"),
    )
        .into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"LPE RPC\""),
    );
    response
}

pub(crate) fn error_response(error: &anyhow::Error) -> Response {
    let message = error.to_string();
    if is_authentication_error(&message) {
        return soap_auth_challenge(&message);
    }
    soap_error(StatusCode::BAD_REQUEST, &message)
}

fn is_authentication_error(message: &str) -> bool {
    matches!(
        message,
        "missing account authentication" | "invalid credentials"
    ) || message.contains("oauth access token")
}

fn soap_auth_challenge(message: &str) -> Response {
    let mut response = soap_error(StatusCode::UNAUTHORIZED, message);
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"LPE EWS\""),
    );
    response
}

fn soap_error(status: StatusCode, message: &str) -> Response {
    let envelope = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">",
            "<s:Body><s:Fault>",
            "<faultcode>s:Client</faultcode>",
            "<faultstring>{}</faultstring>",
            "</s:Fault></s:Body>",
            "</s:Envelope>"
        ),
        escape_xml(message)
    );
    xml_response(status, envelope)
}

fn xml_response(status: StatusCode, body: String) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-8"),
    );
    response
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
