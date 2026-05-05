use anyhow::{anyhow, Result};
use axum::{
    http::{
        header::{CONTENT_TYPE, SET_COOKIE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{JmapEmail, JmapEmailAddress, JmapMailbox};
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime},
};
use uuid::Uuid;

use crate::store::ExchangeStore;

const MAPI_CONTENT_TYPE: &str = "application/mapi-http";
const MAPI_OCTET_STREAM_CONTENT_TYPE: &str = "application/octet-stream";
const MAPI_SERVER_APPLICATION: &str = "LPE/0.1.3";
const EMSMDB_COOKIE: &str = "lpe_mapi_emsmdb";
const NSPI_COOKIE: &str = "lpe_mapi_nspi";
const EMSMDB_COOKIE_PATH: &str = "/mapi/emsmdb";
const NSPI_COOKIE_PATH: &str = "/mapi/nspi";
const MAPI_SESSION_MAX_AGE_SECONDS: u32 = 1_800;
const NSPI_SERVER_GUID: [u8; 16] = [
    0x4c, 0x50, 0x45, 0x00, 0x4d, 0x41, 0x50, 0x49, 0x4e, 0x53, 0x50, 0x49, 0x00, 0x00, 0x00, 0x01,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MapiEndpoint {
    Emsmdb,
    Nspi,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MapiRequestType {
    Connect,
    Disconnect,
    Execute,
    Bind,
    Unbind,
    CompareMids,
    DnToMid,
    GetMatches,
    GetPropList,
    GetProps,
    GetSpecialTable,
    GetTemplateInfo,
    GetAddressBookUrl,
    GetMailboxUrl,
    QueryColumns,
    QueryRows,
    ResolveNames,
    ResortRestriction,
    SeekEntries,
    UpdateStat,
    Ping,
    Unsupported(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MapiSession {
    endpoint: MapiEndpoint,
    tenant_id: String,
    account_id: Uuid,
    email: String,
    last_seen_at: SystemTime,
    next_handle: u32,
    handles: HashMap<u32, MapiObject>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MapiObject {
    Logon,
    Folder { folder_id: u64 },
    Message { folder_id: u64, message_id: u64 },
    HierarchyTable { folder_id: u64, columns: Vec<u32> },
    ContentsTable { folder_id: u64, columns: Vec<u32> },
}

static MAPI_SESSIONS: OnceLock<Mutex<HashMap<String, MapiSession>>> = OnceLock::new();

fn sessions() -> &'static Mutex<HashMap<String, MapiSession>> {
    MAPI_SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) async fn handle_mapi<S: ExchangeStore>(
    store: &S,
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
    _body: &[u8],
) -> Result<Response> {
    let principal = authenticate_account(store, None, headers, "mapi").await?;
    let request_type = request_type(headers)?;
    let request_id = request_id(headers);
    if !is_mapi_content_type(headers) {
        return Ok(mapi_diagnostic_response(
            request_type.header_value(),
            &request_id,
            4,
            "MAPI requests must use Content-Type application/mapi-http.",
        ));
    }

    let response = match (endpoint, request_type) {
        (MapiEndpoint::Emsmdb, MapiRequestType::Connect) => {
            connect_response(endpoint, &principal, &request_id)
        }
        (MapiEndpoint::Emsmdb, MapiRequestType::Disconnect) => {
            disconnect_response(endpoint, &principal, headers, &request_id, "Disconnect")
        }
        (MapiEndpoint::Emsmdb, MapiRequestType::Execute) => {
            execute_response(store, endpoint, &principal, headers, _body, &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::Bind) => {
            bind_response(endpoint, &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::Unbind) => {
            disconnect_response(endpoint, &principal, headers, &request_id, "Unbind")
        }
        (MapiEndpoint::Nspi, MapiRequestType::CompareMids) => {
            nspi_u32_result_response("CompareMIds", &request_id, 0)
        }
        (MapiEndpoint::Nspi, MapiRequestType::DnToMid) => nspi_u32_result_response(
            "DNToMId",
            &request_id,
            principal_minimal_entry_id(&principal),
        ),
        (MapiEndpoint::Nspi, MapiRequestType::GetMatches) => {
            nspi_principal_rowset_response("GetMatches", &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetPropList) => {
            nspi_property_tags_response("GetPropList", &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetProps) => {
            nspi_principal_props_response("GetProps", &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetSpecialTable) => {
            nspi_special_table_response(&request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetTemplateInfo) => {
            nspi_principal_props_response("GetTemplateInfo", &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetAddressBookUrl) => {
            endpoint_url_response("GetAddressBookUrl", &request_id, headers, "/mapi/nspi/")
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetMailboxUrl) => {
            endpoint_url_response("GetMailboxUrl", &request_id, headers, "/mapi/emsmdb/")
        }
        (MapiEndpoint::Nspi, MapiRequestType::QueryColumns) => {
            nspi_property_tags_response("QueryColumns", &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::QueryRows) => {
            nspi_principal_rowset_response("QueryRows", &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::ResolveNames) => {
            resolve_names_response(&principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::ResortRestriction) => {
            nspi_principal_rowset_response("ResortRestriction", &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::SeekEntries) => {
            nspi_principal_rowset_response("SeekEntries", &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::UpdateStat) => nspi_update_stat_response(&request_id),
        (_, MapiRequestType::Ping) => mapi_response("PING", &request_id, 0, Vec::new(), None),
        (_, MapiRequestType::Unsupported(value)) => mapi_diagnostic_response(
            &value,
            &request_id,
            16,
            &format!("MAPI request type {value} is not implemented by LPE yet."),
        ),
        (MapiEndpoint::Emsmdb, other) => mapi_diagnostic_response(
            other.header_value(),
            &request_id,
            5,
            "request type is not valid for the EMSMDB endpoint",
        ),
        (MapiEndpoint::Nspi, other) => mapi_diagnostic_response(
            other.header_value(),
            &request_id,
            5,
            "request type is not valid for the NSPI endpoint",
        ),
    };

    Ok(response)
}

pub(crate) fn mapi_error_response(error: &anyhow::Error) -> Response {
    let message = error.to_string();
    if is_authentication_error(&message) {
        let mut response = StatusCode::UNAUTHORIZED.into_response();
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=\"LPE MAPI\""),
        );
        return response;
    }

    mapi_diagnostic_response("Unknown", "", 4, &message)
}

fn connect_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    request_id: &str,
) -> Response {
    let session_id = create_session(endpoint, principal);
    let cookie = session_cookie(endpoint, &session_id, false);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 30_000);
    write_u32(&mut body, 3);
    write_u32(&mut body, 1_000);
    body.extend_from_slice(b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients\0");
    write_utf16z(&mut body, &principal.display_name);
    write_u32(&mut body, 0);
    mapi_response("Connect", request_id, 0, body, Some(cookie))
}

fn bind_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    request_id: &str,
) -> Response {
    let session_id = create_session(endpoint, principal);
    let cookie = session_cookie(endpoint, &session_id, false);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.extend_from_slice(&NSPI_SERVER_GUID);
    write_u32(&mut body, 0);
    mapi_response("Bind", request_id, 0, body, Some(cookie))
}

async fn execute_response<S: ExchangeStore>(
    store: &S,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    body: &[u8],
    request_id: &str,
) -> Response {
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return execute_failure_response(request_id, 13, "missing MAPI session cookie", None);
    };
    let Some(session) = get_session(&session_id) else {
        return execute_failure_response(request_id, 10, "MAPI session context not found", None);
    };
    if session.endpoint != endpoint
        || session.tenant_id != principal.tenant_id
        || session.account_id != principal.account_id
        || session.email != principal.email
    {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            None,
        );
    }

    let execute = match parse_execute_request(body) {
        Ok(execute) => execute,
        Err(error) => {
            return execute_failure_response(
                request_id,
                4,
                &format!("invalid Execute request body: {error}"),
                Some(session_cookie(endpoint, &session_id, false)),
            );
        }
    };
    let mailboxes = match store.fetch_jmap_mailboxes(principal.account_id).await {
        Ok(mailboxes) => mailboxes,
        Err(error) => {
            return execute_failure_response(
                request_id,
                4,
                &format!("failed to load mailbox folders: {error}"),
                Some(session_cookie(endpoint, &session_id, false)),
            );
        }
    };
    let emails = match store
        .query_jmap_email_ids(principal.account_id, None, None, 0, 500)
        .await
    {
        Ok(query) => match store
            .fetch_jmap_emails(principal.account_id, &query.ids)
            .await
        {
            Ok(emails) => emails,
            Err(error) => {
                return execute_failure_response(
                    request_id,
                    4,
                    &format!("failed to load mailbox messages: {error}"),
                    Some(session_cookie(endpoint, &session_id, false)),
                );
            }
        },
        Err(error) => {
            return execute_failure_response(
                request_id,
                4,
                &format!("failed to query mailbox messages: {error}"),
                Some(session_cookie(endpoint, &session_id, false)),
            );
        }
    };
    let Some(rop_buffer) = with_session_mut(&session_id, |session| {
        if !session_matches(session, endpoint, principal) {
            return None;
        }
        Some(execute_rops(
            principal,
            session,
            &mailboxes,
            &emails,
            &execute.rop_buffer,
        ))
    })
    .flatten() else {
        return execute_failure_response(
            request_id,
            10,
            "MAPI session context not found",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    };
    let response_body = execute_success_body(rop_buffer, Vec::new());
    mapi_response(
        "Execute",
        request_id,
        0,
        response_body,
        Some(session_cookie(endpoint, &session_id, false)),
    )
}

fn disconnect_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
    response_request_type: &str,
) -> Response {
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return mapi_diagnostic_response(
            response_request_type,
            request_id,
            13,
            "missing MAPI session cookie",
        );
    };
    let Some(session) = remove_session(&session_id) else {
        return mapi_diagnostic_response(
            response_request_type,
            request_id,
            10,
            "MAPI session context not found",
        );
    };
    if session.endpoint != endpoint
        || session.tenant_id != principal.tenant_id
        || session.account_id != principal.account_id
        || session.email != principal.email
    {
        return mapi_diagnostic_response(
            response_request_type,
            request_id,
            10,
            "MAPI authentication context changed",
        );
    }

    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    mapi_response(
        response_request_type,
        request_id,
        0,
        body,
        Some(session_cookie(endpoint, "", true)),
    )
}

fn endpoint_url_response(
    request_type: &str,
    request_id: &str,
    headers: &HeaderMap,
    path: &str,
) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_utf16z(&mut body, &public_endpoint_url(headers, path));
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

const NSPI_BOOTSTRAP_PROPERTY_TAGS: &[u32] = &[
    0x3001_001F, // PidTagDisplayName
    0x39FE_001F, // PidTagSmtpAddress
    0x3003_001F, // PidTagEmailAddress
    0x3A00_001F, // PidTagAccount
    0x0FFE_0003, // PidTagObjectType
    0x3000_0003, // PidTagRowId
    0x3004_001F, // PidTagComment
    0x3002_001F, // PidTagAddressType / legacy bootstrap metadata
];

fn resolve_names_response(principal: &AccountPrincipal, request_id: &str) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 1);
    write_u32(&mut body, principal_minimal_entry_id(principal));
    write_u32(&mut body, 1);
    body.extend_from_slice(&nspi_resolved_principal_row(principal));
    write_u32(&mut body, 0);
    mapi_response("ResolveNames", request_id, 0, body, None)
}

fn nspi_u32_result_response(request_type: &str, request_id: &str, value: u32) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, value);
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

fn nspi_property_tags_response(request_type: &str, request_id: &str) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS.len() as u32);
    for tag in NSPI_BOOTSTRAP_PROPERTY_TAGS {
        write_u32(&mut body, *tag);
    }
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

fn nspi_principal_props_response(
    request_type: &str,
    principal: &AccountPrincipal,
    request_id: &str,
) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.extend_from_slice(&nspi_resolved_principal_row(principal));
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

fn nspi_principal_rowset_response(
    request_type: &str,
    principal: &AccountPrincipal,
    request_id: &str,
) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 1);
    body.extend_from_slice(&nspi_resolved_principal_row(principal));
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

fn nspi_special_table_response(request_id: &str) -> Response {
    let mut table_row = Vec::new();
    write_u32(&mut table_row, 4);
    write_nspi_string_property(&mut table_row, 0x3001_001F, "Global Address List");
    write_nspi_u32_property(&mut table_row, 0x0FFE_0003, 0);
    write_nspi_u32_property(&mut table_row, 0x3000_0003, 1);
    write_nspi_string_property(
        &mut table_row,
        0x3002_001F,
        "/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Address Lists/cn=Global Address List",
    );

    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 1);
    write_u32(&mut body, 1);
    body.extend_from_slice(&table_row);
    write_u32(&mut body, 0);
    mapi_response("GetSpecialTable", request_id, 0, body, None)
}

fn nspi_update_stat_response(request_id: &str) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.extend_from_slice(&[0; 36]);
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    mapi_response("UpdateStat", request_id, 0, body, None)
}

fn principal_minimal_entry_id(principal: &AccountPrincipal) -> u32 {
    let bytes = principal.account_id.as_bytes();
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) | 0x8000_0000
}

fn nspi_resolved_principal_row(principal: &AccountPrincipal) -> Vec<u8> {
    let mut row = Vec::new();
    write_u32(&mut row, 8);
    write_nspi_string_property(&mut row, 0x3001_001F, &principal.display_name);
    write_nspi_string_property(&mut row, 0x39FE_001F, &principal.email);
    write_nspi_string_property(&mut row, 0x3003_001F, &principal.email);
    write_nspi_string_property(&mut row, 0x3A00_001F, &principal.display_name);
    write_nspi_u32_property(&mut row, 0x0FFE_0003, principal_minimal_entry_id(principal));
    write_nspi_u32_property(&mut row, 0x3000_0003, principal_minimal_entry_id(principal));
    write_nspi_string_property(&mut row, 0x3004_001F, &principal.email);
    write_nspi_string_property(&mut row, 0x3002_001F, &principal_legacy_dn(principal));
    row
}

fn write_nspi_u32_property(row: &mut Vec<u8>, property_tag: u32, value: u32) {
    write_u32(row, property_tag);
    write_u32(row, 0);
    write_u32(row, value);
}

fn write_nspi_string_property(row: &mut Vec<u8>, property_tag: u32, value: &str) {
    write_u32(row, property_tag);
    write_u32(row, 0);
    write_utf16z(row, value);
}

fn principal_legacy_dn(principal: &AccountPrincipal) -> String {
    let legacy_user = principal
        .email
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={legacy_user}")
}

fn public_endpoint_url(headers: &HeaderMap, path: &str) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("https");
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get("host"))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("localhost");
    format!("{scheme}://{host}{path}")
}

fn create_session(endpoint: MapiEndpoint, principal: &AccountPrincipal) -> String {
    let session_id = Uuid::new_v4().to_string();
    let now = SystemTime::now();
    let session = MapiSession {
        endpoint,
        tenant_id: principal.tenant_id.clone(),
        account_id: principal.account_id,
        email: principal.email.clone(),
        last_seen_at: now,
        next_handle: 1,
        handles: HashMap::new(),
    };
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.insert(session_id.clone(), session);
    session_id
}

fn remove_session(session_id: &str) -> Option<MapiSession> {
    let now = SystemTime::now();
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.remove(session_id)
}

fn get_session(session_id: &str) -> Option<MapiSession> {
    let now = SystemTime::now();
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.get(session_id).cloned()
}

fn with_session_mut<T>(session_id: &str, f: impl FnOnce(&mut MapiSession) -> T) -> Option<T> {
    let now = SystemTime::now();
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.get_mut(session_id).map(|session| {
        session.last_seen_at = now;
        f(session)
    })
}

fn prune_expired_sessions_locked(sessions: &mut HashMap<String, MapiSession>, now: SystemTime) {
    sessions.retain(|_, session| !session_is_expired(session, now));
}

fn session_is_expired(session: &MapiSession, now: SystemTime) -> bool {
    let max_age = Duration::from_secs(u64::from(MAPI_SESSION_MAX_AGE_SECONDS));
    now.duration_since(session.last_seen_at)
        .map(|idle| idle > max_age)
        .unwrap_or(false)
}

fn session_matches(
    session: &MapiSession,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
) -> bool {
    session.endpoint == endpoint
        && session.tenant_id == principal.tenant_id
        && session.account_id == principal.account_id
        && session.email == principal.email
}

fn request_type(headers: &HeaderMap) -> Result<MapiRequestType> {
    let value = headers
        .get("x-requesttype")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing MAPI X-RequestType header"))?;
    Ok(match value.to_ascii_lowercase().as_str() {
        "connect" => MapiRequestType::Connect,
        "disconnect" => MapiRequestType::Disconnect,
        "execute" => MapiRequestType::Execute,
        "bind" => MapiRequestType::Bind,
        "unbind" => MapiRequestType::Unbind,
        "comparemids" => MapiRequestType::CompareMids,
        "dntomid" => MapiRequestType::DnToMid,
        "getmatches" => MapiRequestType::GetMatches,
        "getproplist" => MapiRequestType::GetPropList,
        "getprops" => MapiRequestType::GetProps,
        "getspecialtable" => MapiRequestType::GetSpecialTable,
        "gettemplateinfo" => MapiRequestType::GetTemplateInfo,
        "getaddressbookurl" => MapiRequestType::GetAddressBookUrl,
        "getmailboxurl" => MapiRequestType::GetMailboxUrl,
        "querycolumns" => MapiRequestType::QueryColumns,
        "queryrows" => MapiRequestType::QueryRows,
        "resolvenames" => MapiRequestType::ResolveNames,
        "resortrestriction" => MapiRequestType::ResortRestriction,
        "seekentries" => MapiRequestType::SeekEntries,
        "updatestat" => MapiRequestType::UpdateStat,
        "ping" => MapiRequestType::Ping,
        _ => MapiRequestType::Unsupported(value.to_string()),
    })
}

fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-requestid")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn is_mapi_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|value| {
            value.eq_ignore_ascii_case(MAPI_CONTENT_TYPE)
                || value.eq_ignore_ascii_case(MAPI_OCTET_STREAM_CONTENT_TYPE)
        })
}

fn mapi_diagnostic_response(
    request_type: &str,
    request_id: &str,
    response_code: u16,
    message: &str,
) -> Response {
    mapi_response(
        request_type,
        request_id,
        response_code,
        message.as_bytes().to_vec(),
        None,
    )
}

fn mapi_response(
    request_type: &str,
    request_id: &str,
    response_code: u16,
    body: Vec<u8>,
    cookie: Option<String>,
) -> Response {
    let mut framed_body = Vec::new();
    framed_body.extend_from_slice(b"PROCESSING\r\n");
    framed_body.extend_from_slice(b"DONE\r\n");
    framed_body.extend_from_slice(format!("X-ResponseCode: {response_code}\r\n").as_bytes());
    framed_body.extend_from_slice(b"X-ElapsedTime: 0\r\n");
    framed_body.extend_from_slice(b"X-StartTime: Mon, 01 Jan 2001 00:00:00 GMT\r\n");
    framed_body.extend_from_slice(b"\r\n");
    framed_body.extend_from_slice(&body);

    let mut response = (StatusCode::OK, framed_body).into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(MAPI_CONTENT_TYPE));
    insert_header(&mut response, "x-requesttype", request_type);
    insert_header(&mut response, "x-responsecode", &response_code.to_string());
    insert_header(&mut response, "x-requestid", request_id);
    insert_header(
        &mut response,
        "x-serverapplication",
        MAPI_SERVER_APPLICATION,
    );
    if let Some(cookie) = cookie {
        if let Ok(value) = HeaderValue::from_str(&cookie) {
            response.headers_mut().insert(SET_COOKIE, value);
        }
    }
    response
}

struct ExecuteRequest {
    rop_buffer: Vec<u8>,
}

fn parse_execute_request(body: &[u8]) -> Result<ExecuteRequest> {
    let mut cursor = Cursor::new(body);
    let _flags = cursor.read_u32()?;
    let rop_buffer_size = cursor.read_u32()? as usize;
    let rop_buffer = cursor.read_bytes(rop_buffer_size)?.to_vec();
    let _max_rop_out = cursor.read_u32()?;
    let auxiliary_buffer_size = cursor.read_u32()? as usize;
    let _auxiliary_buffer = cursor.read_bytes(auxiliary_buffer_size)?;
    Ok(ExecuteRequest { rop_buffer })
}

fn execute_rops(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    rop_buffer: &[u8],
) -> Vec<u8> {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return rop_buffer_with_response(unsupported_rop_response(0, 0), &[]);
    };
    let mut handle_slots = read_handle_table(handle_table);

    let mut cursor = Cursor::new(requests);
    let mut responses = Vec::new();
    let mut output_handles = Vec::new();
    while cursor.remaining() > 0 {
        let request = match read_rop_request(&mut cursor) {
            Ok(request) => request,
            Err(_) => {
                responses.extend_from_slice(&unsupported_rop_response(0, 0));
                break;
            }
        };
        match request.rop_id {
            0x01 => {}
            0x02 => {
                let folder_id = request.folder_id().unwrap_or(ROOT_FOLDER_ID);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::Folder { folder_id },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_open_folder_response(&request));
                output_handles.push(handle);
            }
            0x03 => {
                let folder_id = request.folder_id().unwrap_or(INBOX_FOLDER_ID);
                let message_id = request.message_id().unwrap_or(0);
                if let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(&request, email));
                    output_handles.push(handle);
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x03,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            0x04 => {
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(|object| object.folder_id())
                    .unwrap_or(ROOT_FOLDER_ID);
                let columns = default_hierarchy_columns();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::HierarchyTable { folder_id, columns },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_hierarchy_table_response(
                    &request,
                    hierarchy_row_count(folder_id, mailboxes),
                ));
                output_handles.push(handle);
            }
            0x05 => {
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(|object| object.folder_id())
                    .unwrap_or(INBOX_FOLDER_ID);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::ContentsTable {
                        folder_id,
                        columns: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_contents_table_response(
                    &request,
                    folder_message_count(folder_id, mailboxes, emails),
                ));
                output_handles.push(handle);
            }
            0x07 => responses.extend_from_slice(&rop_get_properties_specific_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
            )),
            0x09 => responses.extend_from_slice(&rop_get_properties_list_response(
                &request,
                input_object(session, &handle_slots, &request),
            )),
            0x0F => responses.extend_from_slice(&rop_read_recipients_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
            )),
            0x12 => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::HierarchyTable { columns, .. })
                    | Some(MapiObject::ContentsTable { columns, .. }) => {
                        *columns = request.property_tags();
                    }
                    _ => {}
                }
                responses.extend_from_slice(&rop_set_columns_response(&request));
            }
            0x15 => responses.extend_from_slice(&rop_query_rows_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
            )),
            0x16 => responses.extend_from_slice(&rop_get_status_response(&request)),
            0x17 => responses.extend_from_slice(&rop_query_position_response(&request)),
            0x27 => responses.extend_from_slice(&rop_get_receive_folder_response(&request)),
            0x68 => responses.extend_from_slice(&rop_get_receive_folder_table_response(&request)),
            0x7B => responses.extend_from_slice(&rop_get_store_state_response(&request)),
            0x81 => responses.extend_from_slice(&rop_reset_table_response(&request)),
            0xFE => {
                let handle =
                    session.allocate_output_handle(request.output_handle_index, MapiObject::Logon);
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_logon_response_body(principal, &request));
                output_handles.push(handle);
            }
            rop_id => responses.extend_from_slice(&unsupported_rop_response(
                rop_id,
                request.response_handle_index(),
            )),
        }
    }
    rop_buffer_with_response(responses, &output_handles)
}

const ROOT_FOLDER_ID: u64 = 1;
const IPM_SUBTREE_FOLDER_ID: u64 = 4;
const INBOX_FOLDER_ID: u64 = 5;
const DRAFTS_FOLDER_ID: u64 = 6;
const SENT_FOLDER_ID: u64 = 7;
const TRASH_FOLDER_ID: u64 = 8;

const PID_TAG_DISPLAY_NAME_W: u32 = 0x3001_001F;
const PID_TAG_CONTENT_COUNT: u32 = 0x3602_0003;
const PID_TAG_CONTENT_UNREAD_COUNT: u32 = 0x3603_0003;
const PID_TAG_SUBFOLDERS: u32 = 0x360A_000B;
const PID_TAG_FOLDER_ID: u32 = 0x6748_0014;
const PID_TAG_PARENT_FOLDER_ID: u32 = 0x6749_0014;
const PID_TAG_MESSAGE_CLASS_W: u32 = 0x001A_001F;
const PID_TAG_SUBJECT_W: u32 = 0x0037_001F;
const PID_TAG_SENDER_NAME_W: u32 = 0x0C1A_001F;
const PID_TAG_SENDER_EMAIL_ADDRESS_W: u32 = 0x0C1F_001F;
const PID_TAG_DISPLAY_TO_W: u32 = 0x0E04_001F;
const PID_TAG_MESSAGE_DELIVERY_TIME: u32 = 0x0E06_0040;
const PID_TAG_MESSAGE_FLAGS: u32 = 0x0E07_0003;
const PID_TAG_MESSAGE_SIZE: u32 = 0x0E08_0003;
const PID_TAG_HAS_ATTACHMENTS: u32 = 0x0E1B_000B;
const PID_TAG_NORMALIZED_SUBJECT_W: u32 = 0x0E1D_001F;
const PID_TAG_INSTANCE_KEY: u32 = 0x0FF6_0102;
const PID_TAG_ENTRY_ID: u32 = 0x0FFF_0102;
const PID_TAG_BODY_W: u32 = 0x1000_001F;
const PID_TAG_INTERNET_MESSAGE_ID_W: u32 = 0x1035_001F;
const PID_TAG_LAST_MODIFICATION_TIME: u32 = 0x3008_0040;
const PID_TAG_MID: u32 = 0x674A_0014;

impl MapiSession {
    fn allocate_output_handle(
        &mut self,
        output_handle_index: Option<u8>,
        object: MapiObject,
    ) -> u32 {
        let preferred = output_handle_index.map(|index| index as u32 + 1);
        let handle = preferred
            .filter(|handle| !self.handles.contains_key(handle))
            .unwrap_or(self.next_handle);
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        if handle >= self.next_handle {
            self.next_handle = handle.saturating_add(1).max(1);
        }
        self.handles.insert(handle, object);
        handle
    }
}

impl MapiObject {
    fn folder_id(&self) -> Option<u64> {
        match self {
            MapiObject::Logon => Some(ROOT_FOLDER_ID),
            MapiObject::Folder { folder_id }
            | MapiObject::Message { folder_id, .. }
            | MapiObject::HierarchyTable { folder_id, .. }
            | MapiObject::ContentsTable { folder_id, .. } => Some(*folder_id),
        }
    }
}

fn input_object<'a>(
    session: &'a MapiSession,
    input_handles: &[u32],
    request: &RopRequest,
) -> Option<&'a MapiObject> {
    let handle = input_handle(input_handles, request)?;
    session.handles.get(&handle)
}

fn input_object_mut<'a>(
    session: &'a mut MapiSession,
    input_handles: &[u32],
    request: &RopRequest,
) -> Option<&'a mut MapiObject> {
    let handle = input_handle(input_handles, request)?;
    session.handles.get_mut(&handle)
}

fn input_handle(input_handles: &[u32], request: &RopRequest) -> Option<u32> {
    input_handles
        .get(request.input_handle_index()? as usize)
        .copied()
        .filter(|handle| *handle != u32::MAX)
}

fn set_handle_slot(handle_slots: &mut Vec<u32>, output_handle_index: Option<u8>, handle: u32) {
    let Some(index) = output_handle_index.map(usize::from) else {
        return;
    };
    if handle_slots.len() <= index {
        handle_slots.resize(index + 1, u32::MAX);
    }
    handle_slots[index] = handle;
}

fn read_handle_table(handle_table: &[u8]) -> Vec<u32> {
    handle_table
        .chunks_exact(4)
        .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        .collect()
}

fn hierarchy_row_count(folder_id: u64, mailboxes: &[JmapMailbox]) -> u32 {
    if is_root_hierarchy_folder(folder_id) {
        mailboxes.len() as u32
    } else {
        0
    }
}

fn folder_message_count(folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail]) -> u32 {
    emails_for_folder(folder_id, mailboxes, emails).len() as u32
}

fn default_hierarchy_columns() -> Vec<u32> {
    vec![
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_SUBFOLDERS,
    ]
}

fn default_contents_columns() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
    ]
}

fn default_folder_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_SUBFOLDERS,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_LAST_MODIFICATION_TIME,
    ]
}

fn default_message_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_BODY_W,
        PID_TAG_INTERNET_MESSAGE_ID_W,
    ]
}

fn split_rop_buffer(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    if buffer.len() < 2 {
        return None;
    }
    let rop_size = u16::from_le_bytes([buffer[0], buffer[1]]) as usize;
    if buffer.len() < 2 + rop_size {
        return None;
    }
    Some((&buffer[2..2 + rop_size], &buffer[2 + rop_size..]))
}

fn rop_logon_response_body(principal: &AccountPrincipal, request: &RopRequest) -> Vec<u8> {
    let output_handle_index = request.output_handle_index.unwrap_or(0);
    let logon_flags = request.payload.first().copied().unwrap_or(0x01) | 0x01;
    let mut response = Vec::new();
    response.push(0xFE);
    response.push(output_handle_index);
    write_u32(&mut response, 0);
    response.push(logon_flags);
    for folder_id in 1..=13u64 {
        write_u64(&mut response, folder_id);
    }
    response.push(0x03);
    response.extend_from_slice(principal.account_id.as_bytes());
    response.extend_from_slice(&1u16.to_le_bytes());
    response.extend_from_slice(principal.account_id.as_bytes());
    response.extend_from_slice(&[0u8; 8]);
    response.extend_from_slice(&[0u8; 8]);
    write_u32(&mut response, 0);
    response
}

fn rop_open_folder_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x02, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    response.push(0);
    response
}

fn rop_open_message_response(request: &RopRequest, email: &JmapEmail) -> Vec<u8> {
    let mut response = vec![0x03, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    write_typed_string(&mut response, "");
    write_typed_string(&mut response, &email.subject);
    response.extend_from_slice(&(message_recipients(email).len() as u16).to_le_bytes());
    response.extend_from_slice(&0u16.to_le_bytes());
    response.push(0);
    response
}

fn rop_get_hierarchy_table_response(request: &RopRequest, row_count: u32) -> Vec<u8> {
    let mut response = vec![0x04, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u32(&mut response, row_count);
    response
}

fn rop_get_contents_table_response(request: &RopRequest, row_count: u32) -> Vec<u8> {
    let mut response = vec![0x05, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u32(&mut response, row_count);
    response
}

fn rop_set_columns_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x12, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

fn rop_get_properties_list_response(request: &RopRequest, object: Option<&MapiObject>) -> Vec<u8> {
    let tags = match object {
        Some(MapiObject::Message { .. }) => default_message_property_tags(),
        _ => default_folder_property_tags(),
    };
    let mut response = vec![0x09, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        write_u32(&mut response, tag);
    }
    response
}

fn rop_get_properties_specific_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<u8> {
    let mut response = vec![0x07, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let columns = request.property_tags();
    let row = match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
        }) => {
            let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_message_row(email, &columns)
        }
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| serialize_folder_row(mailbox, &columns))
                .unwrap_or_else(|| serialize_root_folder_row(mailboxes, &columns))
        }
    };
    response.extend_from_slice(&row);
    response
}

fn rop_read_recipients_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<u8> {
    let input_handle_index = request.input_handle_index().unwrap_or(0);
    let Some(MapiObject::Message {
        folder_id,
        message_id,
    }) = object
    else {
        return rop_error_response(0x0F, input_handle_index, 0x0000_04B9);
    };
    let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) else {
        return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
    };
    let start = request.row_id().unwrap_or(0) as usize;
    let recipients = message_recipients(email);
    if start >= recipients.len() {
        return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
    }

    let mut response = vec![0x0F, input_handle_index];
    write_u32(&mut response, 0);
    for (offset, recipient) in recipients.into_iter().enumerate().skip(start) {
        write_u32(&mut response, offset as u32);
        response.push(recipient.recipient_type);
        response.extend_from_slice(&0x0FFFu16.to_le_bytes());
        response.extend_from_slice(&0u16.to_le_bytes());
        let row = serialize_recipient_row(recipient.address);
        response.extend_from_slice(&(row.len() as u16).to_le_bytes());
        response.extend_from_slice(&row);
    }
    response
}

fn rop_query_rows_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<u8> {
    let mut response = vec![0x15, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0x02);
    let rows = match object {
        Some(MapiObject::HierarchyTable { folder_id, columns })
            if is_root_hierarchy_folder(*folder_id) =>
        {
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            mailboxes
                .iter()
                .map(|mailbox| serialize_folder_row(mailbox, &columns))
                .collect::<Vec<_>>()
        }
        Some(MapiObject::ContentsTable { folder_id, columns }) => {
            let columns = if columns.is_empty() {
                default_contents_columns()
            } else {
                columns.clone()
            };
            emails_for_folder(*folder_id, mailboxes, emails)
                .into_iter()
                .map(|email| serialize_message_row(email, &columns))
                .collect::<Vec<_>>()
        }
        _ => Vec::new(),
    };
    response.extend_from_slice(&(rows.len() as u16).to_le_bytes());
    for row in rows {
        response.extend_from_slice(&row);
    }
    response
}

fn rop_get_status_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x16, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

fn rop_query_position_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x17, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, 0);
    write_u32(&mut response, 0);
    response
}

fn rop_get_receive_folder_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x27, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u64(&mut response, INBOX_FOLDER_ID);
    response.extend_from_slice(b"IPM.Note\0");
    response
}

fn rop_get_receive_folder_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x68, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, 1);
    response.push(0);
    write_u64(&mut response, INBOX_FOLDER_ID);
    write_utf16z(&mut response, "IPM.Note");
    write_u64(&mut response, 0);
    response
}

fn rop_get_store_state_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x7B, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, 0);
    response
}

fn rop_reset_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x81, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

fn folder_row_for_id(folder_id: u64, mailboxes: &[JmapMailbox]) -> Option<&JmapMailbox> {
    mailboxes.iter().find(|mailbox| {
        mapi_folder_id(mailbox) == folder_id
            || mailbox.role == role_for_folder_id(folder_id).unwrap_or_default()
    })
}

fn emails_for_folder<'a>(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &'a [JmapEmail],
) -> Vec<&'a JmapEmail> {
    emails
        .iter()
        .filter(|email| email_matches_folder(email, folder_id, mailboxes))
        .collect()
}

fn message_for_id<'a>(
    folder_id: u64,
    message_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &'a [JmapEmail],
) -> Option<&'a JmapEmail> {
    emails.iter().find(|email| {
        mapi_message_id(email) == message_id && email_matches_folder(email, folder_id, mailboxes)
    })
}

fn email_matches_folder(email: &JmapEmail, folder_id: u64, mailboxes: &[JmapMailbox]) -> bool {
    if let Some(role) = role_for_folder_id(folder_id) {
        return email.mailbox_role == role;
    }

    mailboxes
        .iter()
        .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
        .is_some_and(|mailbox| email.mailbox_id == mailbox.id)
}

fn is_root_hierarchy_folder(folder_id: u64) -> bool {
    matches!(folder_id, ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID)
}

fn role_for_folder_id(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        INBOX_FOLDER_ID => Some("inbox"),
        DRAFTS_FOLDER_ID => Some("drafts"),
        SENT_FOLDER_ID => Some("sent"),
        TRASH_FOLDER_ID => Some("trash"),
        _ => None,
    }
}

fn serialize_root_folder_row(mailboxes: &[JmapMailbox], columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, "Root"),
            PID_TAG_FOLDER_ID => write_u64(&mut row, ROOT_FOLDER_ID),
            PID_TAG_PARENT_FOLDER_ID => write_u64(&mut row, 0),
            PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            PID_TAG_SUBFOLDERS => row.push((!mailboxes.is_empty()) as u8),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPF.Root"),
            PID_TAG_LAST_MODIFICATION_TIME => write_u64(&mut row, 0),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn serialize_folder_row(mailbox: &JmapMailbox, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &mailbox.name),
            PID_TAG_FOLDER_ID => write_u64(&mut row, mapi_folder_id(mailbox)),
            PID_TAG_PARENT_FOLDER_ID => write_u64(&mut row, ROOT_FOLDER_ID),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, mailbox.total_emails),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, mailbox.unread_emails),
            PID_TAG_SUBFOLDERS => row.push(0),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, folder_message_class(mailbox)),
            PID_TAG_LAST_MODIFICATION_TIME => write_u64(&mut row, 0),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn serialize_message_row(email: &JmapEmail, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_MID => write_u64(&mut row, mapi_message_id(email)),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                write_utf16z(&mut row, &email.subject)
            }
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPM.Note"),
            PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                write_u64(&mut row, 0)
            }
            PID_TAG_MESSAGE_FLAGS => write_u32(&mut row, message_flags(email)),
            PID_TAG_MESSAGE_SIZE => {
                write_u32(&mut row, email.size_octets.clamp(0, u32::MAX as i64) as u32)
            }
            PID_TAG_SENDER_NAME_W => write_utf16z(
                &mut row,
                email.from_display.as_deref().unwrap_or(&email.from_address),
            ),
            PID_TAG_SENDER_EMAIL_ADDRESS_W => write_utf16z(&mut row, &email.from_address),
            PID_TAG_DISPLAY_TO_W => write_utf16z(&mut row, &display_to(email)),
            PID_TAG_HAS_ATTACHMENTS => row.push(email.has_attachments as u8),
            PID_TAG_BODY_W => write_utf16z(&mut row, &email.body_text),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
                write_u16_prefixed_bytes(&mut row, email.id.as_bytes())
            }
            PID_TAG_INTERNET_MESSAGE_ID_W => {
                write_utf16z(&mut row, email.internet_message_id.as_deref().unwrap_or(""))
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn display_to(email: &JmapEmail) -> String {
    email
        .to
        .iter()
        .map(|address| {
            address
                .display_name
                .as_deref()
                .unwrap_or(&address.address)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("; ")
}

struct MapiRecipient<'a> {
    recipient_type: u8,
    address: &'a JmapEmailAddress,
}

fn message_recipients(email: &JmapEmail) -> Vec<MapiRecipient<'_>> {
    email
        .to
        .iter()
        .map(|address| MapiRecipient {
            recipient_type: 0x01,
            address,
        })
        .chain(email.cc.iter().map(|address| MapiRecipient {
            recipient_type: 0x02,
            address,
        }))
        .collect()
}

fn serialize_recipient_row(address: &JmapEmailAddress) -> Vec<u8> {
    let mut row = Vec::new();
    let recipient_flags = 0x0200u16 | 0x0010 | 0x0008 | 0x0003;
    row.extend_from_slice(&recipient_flags.to_le_bytes());
    write_utf16z(&mut row, &address.address);
    write_utf16z(
        &mut row,
        address.display_name.as_deref().unwrap_or(&address.address),
    );
    row.extend_from_slice(&0u16.to_le_bytes());
    row
}

fn message_flags(email: &JmapEmail) -> u32 {
    let mut flags = 0u32;
    if !email.unread {
        flags |= 0x0000_0001;
    }
    if email.has_attachments {
        flags |= 0x0000_0010;
    }
    flags
}

fn folder_message_class(mailbox: &JmapMailbox) -> &'static str {
    match mailbox.role.as_str() {
        "contacts" => "IPF.Contact",
        "calendar" => "IPF.Appointment",
        _ => "IPF.Note",
    }
}

fn write_property_default(row: &mut Vec<u8>, property_tag: u32) {
    match property_tag & 0xFFFF {
        0x0003 => write_u32(row, 0),
        0x000B => row.push(0),
        0x0014 => write_u64(row, 0),
        0x001E | 0x001F => write_utf16z(row, ""),
        0x0040 => write_u64(row, 0),
        0x0048 => row.extend_from_slice(Uuid::nil().as_bytes()),
        0x0102 => write_u16_prefixed_bytes(row, &[]),
        _ => write_u32(row, 0x8004_0102),
    }
}

fn mapi_folder_id(mailbox: &JmapMailbox) -> u64 {
    let bytes = mailbox.id.as_bytes();
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]) | 0x8000_0000_0000_0000
}

fn mapi_message_id(email: &JmapEmail) -> u64 {
    let bytes = email.id.as_bytes();
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]) | 0x4000_0000_0000_0000
}

fn unsupported_rop_response(rop_id: u8, handle_index: u8) -> Vec<u8> {
    rop_error_response(rop_id, handle_index, 0x8004_0102)
}

fn rop_error_response(rop_id: u8, handle_index: u8, error_code: u32) -> Vec<u8> {
    let mut response = vec![rop_id, handle_index];
    write_u32(&mut response, error_code);
    response
}

fn rop_buffer_with_response(response: Vec<u8>, output_handles: &[u32]) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&(response.len() as u16).to_le_bytes());
    buffer.extend_from_slice(&response);
    for handle in output_handles {
        buffer.extend_from_slice(&handle.to_le_bytes());
    }
    buffer
}

fn execute_success_body(rop_buffer: Vec<u8>, auxiliary_buffer: Vec<u8>) -> Vec<u8> {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, rop_buffer.len() as u32);
    body.extend_from_slice(&rop_buffer);
    write_u32(&mut body, auxiliary_buffer.len() as u32);
    body.extend_from_slice(&auxiliary_buffer);
    body
}

fn execute_failure_response(
    request_id: &str,
    status_code: u32,
    message: &str,
    cookie: Option<String>,
) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, status_code);
    write_u32(&mut body, message.len() as u32);
    body.extend_from_slice(message.as_bytes());
    mapi_response("Execute", request_id, status_code as u16, body, cookie)
}

fn insert_header(response: &mut Response, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        response.headers_mut().insert(name, value);
    }
}

fn request_cookie(endpoint: MapiEndpoint, headers: &HeaderMap) -> Option<String> {
    let name = cookie_name(endpoint);
    headers
        .get("cookie")
        .and_then(|value| value.to_str().ok())
        .and_then(|cookie| {
            cookie.split(';').find_map(|part| {
                let (key, value) = part.trim().split_once('=')?;
                (key == name && !value.is_empty()).then(|| value.to_string())
            })
        })
}

fn session_cookie(endpoint: MapiEndpoint, session_id: &str, expired: bool) -> String {
    let name = cookie_name(endpoint);
    let path = cookie_path(endpoint);
    if expired {
        format!("{name}=; Path={path}; Max-Age=0; HttpOnly; SameSite=Lax; Secure")
    } else {
        format!(
            "{name}={session_id}; Path={path}; Max-Age={MAPI_SESSION_MAX_AGE_SECONDS}; HttpOnly; SameSite=Lax; Secure"
        )
    }
}

fn cookie_name(endpoint: MapiEndpoint) -> &'static str {
    match endpoint {
        MapiEndpoint::Emsmdb => EMSMDB_COOKIE,
        MapiEndpoint::Nspi => NSPI_COOKIE,
    }
}

fn cookie_path(endpoint: MapiEndpoint) -> &'static str {
    match endpoint {
        MapiEndpoint::Emsmdb => EMSMDB_COOKIE_PATH,
        MapiEndpoint::Nspi => NSPI_COOKIE_PATH,
    }
}

fn write_u32(body: &mut Vec<u8>, value: u32) {
    body.extend_from_slice(&value.to_le_bytes());
}

fn write_u16_prefixed_bytes(body: &mut Vec<u8>, value: &[u8]) {
    body.extend_from_slice(&(value.len() as u16).to_le_bytes());
    body.extend_from_slice(value);
}

fn write_u64(body: &mut Vec<u8>, value: u64) {
    body.extend_from_slice(&value.to_le_bytes());
}

fn write_utf16z(body: &mut Vec<u8>, value: &str) {
    for unit in value.encode_utf16() {
        body.extend_from_slice(&unit.to_le_bytes());
    }
    body.extend_from_slice(&0u16.to_le_bytes());
}

fn write_typed_string(body: &mut Vec<u8>, value: &str) {
    if value.is_empty() {
        body.push(0x01);
    } else {
        body.push(0x04);
        write_utf16z(body, value);
    }
}

fn is_authentication_error(message: &str) -> bool {
    matches!(
        message,
        "missing account authentication" | "invalid credentials"
    ) || message.contains("oauth access token")
}

impl MapiRequestType {
    fn header_value(&self) -> &str {
        match self {
            MapiRequestType::Connect => "Connect",
            MapiRequestType::Disconnect => "Disconnect",
            MapiRequestType::Execute => "Execute",
            MapiRequestType::Bind => "Bind",
            MapiRequestType::Unbind => "Unbind",
            MapiRequestType::CompareMids => "CompareMIds",
            MapiRequestType::DnToMid => "DNToMId",
            MapiRequestType::GetMatches => "GetMatches",
            MapiRequestType::GetPropList => "GetPropList",
            MapiRequestType::GetProps => "GetProps",
            MapiRequestType::GetSpecialTable => "GetSpecialTable",
            MapiRequestType::GetTemplateInfo => "GetTemplateInfo",
            MapiRequestType::GetAddressBookUrl => "GetAddressBookUrl",
            MapiRequestType::GetMailboxUrl => "GetMailboxUrl",
            MapiRequestType::QueryColumns => "QueryColumns",
            MapiRequestType::QueryRows => "QueryRows",
            MapiRequestType::ResolveNames => "ResolveNames",
            MapiRequestType::ResortRestriction => "ResortRestriction",
            MapiRequestType::SeekEntries => "SeekEntries",
            MapiRequestType::UpdateStat => "UpdateStat",
            MapiRequestType::Ping => "PING",
            MapiRequestType::Unsupported(value) => value,
        }
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| anyhow!("request body offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| anyhow!("request body is truncated"))?;
        self.position = end;
        Ok(bytes)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }
}

struct RopRequest {
    rop_id: u8,
    input_handle_index: Option<u8>,
    output_handle_index: Option<u8>,
    payload: Vec<u8>,
}

impl RopRequest {
    fn input_handle_index(&self) -> Option<u8> {
        self.input_handle_index
    }

    fn response_handle_index(&self) -> u8 {
        self.input_handle_index
            .unwrap_or(self.output_handle_index.unwrap_or(0))
    }

    fn folder_id(&self) -> Option<u64> {
        let bytes = self.payload.get(..8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    fn message_id(&self) -> Option<u64> {
        let bytes = self.payload.get(9..17)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    fn row_id(&self) -> Option<u32> {
        let bytes = self.payload.get(..4)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    fn property_tags(&self) -> Vec<u32> {
        let start = match self.rop_id {
            0x07 => 4,
            _ => 3,
        };
        if self.payload.len() < start {
            return Vec::new();
        }
        let count_offset = start - 2;
        let count = u16::from_le_bytes([self.payload[count_offset], self.payload[count_offset + 1]])
            as usize;
        self.payload[start..]
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect()
    }
}

fn read_rop_request(cursor: &mut Cursor<'_>) -> Result<RopRequest> {
    let rop_id = cursor.read_u8()?;
    let _logon_id = cursor.read_u8()?;
    match rop_id {
        0x01 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x02 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x03 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let _code_page_id = cursor.read_u16()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x04 | 0x05 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x09 | 0x16 | 0x17 | 0x68 | 0x7B | 0x81 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x0F => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            let _reserved = cursor.read_u16()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x12 => {
            let input_handle_index = cursor.read_u8()?;
            let set_columns_flags = cursor.read_u8()?;
            let property_tag_count = cursor.read_u16()? as usize;
            let mut payload = vec![set_columns_flags];
            payload.extend_from_slice(&(property_tag_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_tag_count * 4)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x07 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            let property_tag_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(property_tag_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_tag_count * 4)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x15 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x27 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            while cursor.remaining() > 0 {
                let byte = cursor.read_u8()?;
                payload.push(byte);
                if byte == 0 {
                    break;
                }
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0xFE => {
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(cursor.read_bytes(4)?);
            payload.extend_from_slice(cursor.read_bytes(4)?);
            let essdn_size = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(essdn_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(essdn_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: None,
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        _ => {
            let input_handle_index = if cursor.remaining() > 0 {
                Some(cursor.read_u8()?)
            } else {
                None
            };
            Ok(RopRequest {
                rop_id,
                input_handle_index,
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_idle_expiry_follows_cookie_max_age() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let fresh = MapiSession {
            endpoint: MapiEndpoint::Emsmdb,
            tenant_id: "tenant".to_string(),
            account_id: Uuid::nil(),
            email: "user@example.test".to_string(),
            last_seen_at: now - Duration::from_secs(u64::from(MAPI_SESSION_MAX_AGE_SECONDS)),
            next_handle: 1,
            handles: HashMap::new(),
        };
        let stale = MapiSession {
            last_seen_at: now - Duration::from_secs(u64::from(MAPI_SESSION_MAX_AGE_SECONDS) + 1),
            ..fresh.clone()
        };

        assert!(!session_is_expired(&fresh, now));
        assert!(session_is_expired(&stale, now));
    }
}
