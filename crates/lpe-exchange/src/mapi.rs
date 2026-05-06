use anyhow::{anyhow, Result};
use axum::{
    http::{
        header::{CONTENT_TYPE, SET_COOKIE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{AuditEntryInput, JmapEmail, JmapEmailAddress, JmapMailbox};
use std::{
    cmp::Ordering,
    collections::HashMap,
    env,
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime},
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    mapi_store::{MapiAttachment, MapiMailStoreSnapshot, MapiStore},
    store::ExchangeStore,
};

const MAPI_CONTENT_TYPE: &str = "application/mapi-http";
const MAPI_OCTET_STREAM_CONTENT_TYPE: &str = "application/octet-stream";
const MAPI_SERVER_APPLICATION: &str = "LPE/0.1.3";
const EMSMDB_COOKIE: &str = "lpe_mapi_emsmdb";
const NSPI_COOKIE: &str = "lpe_mapi_nspi";
const EMSMDB_COOKIE_PATH: &str = "/mapi/emsmdb";
const NSPI_COOKIE_PATH: &str = "/mapi/nspi";
const MAPI_SESSION_MAX_AGE_SECONDS: u32 = 1_800;
const NSPI_UNICODE_CODEPAGE: u32 = 1200;
const MAPI_MAILUSER_OBJECT_TYPE: u32 = 6;
const NSPI_MID_RESOLVED: u32 = 0x0000_0002;
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
struct MapiSortOrder {
    property_tag: u32,
    order: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MapiRestriction {
    And(Vec<MapiRestriction>),
    Or(Vec<MapiRestriction>),
    Not(Box<MapiRestriction>),
    Content {
        property_tag: u32,
        value: String,
    },
    Property {
        relop: u8,
        property_tag: u32,
        value: MapiValue,
    },
    Bitmask {
        property_tag: u32,
        mask: u32,
        must_be_nonzero: bool,
    },
    Size {
        relop: u8,
        property_tag: u32,
        size: u32,
    },
    Exist {
        property_tag: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MapiValue {
    Bool(bool),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MapiObject {
    Logon,
    Folder {
        folder_id: u64,
    },
    Message {
        folder_id: u64,
        message_id: u64,
    },
    HierarchyTable {
        folder_id: u64,
        columns: Vec<u32>,
        sort_orders: Vec<MapiSortOrder>,
        restriction: Option<MapiRestriction>,
        bookmarks: HashMap<Vec<u8>, usize>,
        next_bookmark: u32,
        position: usize,
    },
    ContentsTable {
        folder_id: u64,
        columns: Vec<u32>,
        sort_orders: Vec<MapiSortOrder>,
        restriction: Option<MapiRestriction>,
        bookmarks: HashMap<Vec<u8>, usize>,
        next_bookmark: u32,
        position: usize,
    },
    AttachmentTable {
        folder_id: u64,
        message_id: u64,
        columns: Vec<u32>,
        sort_orders: Vec<MapiSortOrder>,
        restriction: Option<MapiRestriction>,
        bookmarks: HashMap<Vec<u8>, usize>,
        next_bookmark: u32,
        position: usize,
    },
    Attachment {
        folder_id: u64,
        message_id: u64,
        attach_num: u32,
    },
    AttachmentStream {
        data: Vec<u8>,
        position: usize,
    },
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
    let request_type_label = request_type.header_value().to_string();
    let request_id = request_id(headers);
    if !is_mapi_content_type(headers) {
        let response = mapi_diagnostic_response(
            &request_type_label,
            &request_id,
            4,
            "MAPI requests must use Content-Type application/mapi-http.",
        );
        let response = finalize_mapi_response(response, headers);
        log_mapi_connection(
            endpoint,
            &principal,
            headers,
            _body,
            &request_type_label,
            &request_id,
            &response,
        );
        return Ok(response);
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
            nspi_matches_response(&principal, &request_id)
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
            nspi_template_info_response(&principal, &request_id)
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
            resolve_names_response(&principal, _body, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::ResortRestriction) => {
            nspi_minimal_ids_response("ResortRestriction", &principal, &request_id)
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

    let response = finalize_mapi_response(response, headers);
    log_mapi_connection(
        endpoint,
        &principal,
        headers,
        _body,
        &request_type_label,
        &request_id,
        &response,
    );
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
    let snapshot = match store.load_mapi_mail_store(principal.account_id, 500).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return execute_failure_response(
                request_id,
                4,
                &format!("failed to load MAPI mail store snapshot: {error}"),
                Some(session_cookie(endpoint, &session_id, false)),
            );
        }
    };
    let mailboxes = snapshot.mailboxes();
    let emails = snapshot.emails();
    let Some(mut session) = remove_session(&session_id) else {
        return execute_failure_response(
            request_id,
            10,
            "MAPI session context not found",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    };
    if !session_matches(&session, endpoint, principal) {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    }
    let rop_buffer = execute_rops(
        store,
        principal,
        &mut session,
        &mailboxes,
        &emails,
        &snapshot,
        &execute.rop_buffer,
    )
    .await;
    store_session(session_id.clone(), session);
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

fn resolve_names_response(
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
) -> Response {
    let columns = resolve_names_columns(request);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    write_u32(&mut body, 1);
    write_u32(&mut body, NSPI_MID_RESOLVED);
    body.push(1);
    write_large_property_tag_array(&mut body, &columns);
    write_u32(&mut body, 1);
    body.extend_from_slice(&nspi_resolved_principal_row(principal, &columns));
    write_u32(&mut body, 0);
    mapi_response("ResolveNames", request_id, 0, body, None)
}

fn resolve_names_columns(request: &[u8]) -> Vec<u32> {
    parse_resolve_names_columns(request)
        .filter(|columns| !columns.is_empty())
        .unwrap_or_else(|| NSPI_BOOTSTRAP_PROPERTY_TAGS.to_vec())
}

fn parse_resolve_names_columns(request: &[u8]) -> Option<Vec<u32>> {
    let mut cursor = Cursor::new(request);
    let _reserved = cursor.read_u32().ok()?;
    let has_state = cursor.read_u8().ok()? != 0;
    if has_state {
        cursor.read_bytes(36).ok()?;
    }
    let has_property_tags = cursor.read_u8().ok()? != 0;
    if !has_property_tags {
        return None;
    }
    let count = cursor.read_u32().ok()? as usize;
    if count == 0 || count > 128 {
        return None;
    }
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        columns.push(cursor.read_u32().ok()?);
    }
    Some(columns)
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
    body.push(1);
    write_large_property_tag_array(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS);
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
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    body.extend_from_slice(&nspi_principal_property_value_list(
        principal,
        NSPI_BOOTSTRAP_PROPERTY_TAGS,
    ));
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
    body.push(0);
    body.push(1);
    write_large_property_tag_array(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS);
    write_u32(&mut body, 1);
    body.extend_from_slice(&nspi_resolved_principal_row(
        principal,
        NSPI_BOOTSTRAP_PROPERTY_TAGS,
    ));
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

fn nspi_matches_response(principal: &AccountPrincipal, request_id: &str) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push(1);
    write_u32(&mut body, 1);
    write_u32(&mut body, principal_minimal_entry_id(principal));
    body.push(1);
    write_large_property_tag_array(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS);
    write_u32(&mut body, 1);
    body.extend_from_slice(&nspi_resolved_principal_row(
        principal,
        NSPI_BOOTSTRAP_PROPERTY_TAGS,
    ));
    write_u32(&mut body, 0);
    mapi_response("GetMatches", request_id, 0, body, None)
}

fn nspi_minimal_ids_response(
    request_type: &str,
    principal: &AccountPrincipal,
    request_id: &str,
) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push(1);
    write_u32(&mut body, 1);
    write_u32(&mut body, principal_minimal_entry_id(principal));
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

fn nspi_special_table_response(request_id: &str) -> Response {
    let mut table_row = Vec::new();
    write_u32(&mut table_row, 4);
    write_address_book_tagged_property_value(
        &mut table_row,
        0x3001_001F,
        &NspiValue::String("Global Address List"),
    );
    write_address_book_tagged_property_value(&mut table_row, 0x0FFE_0003, &NspiValue::U32(2));
    write_address_book_tagged_property_value(&mut table_row, 0x3000_0003, &NspiValue::U32(1));
    write_address_book_tagged_property_value(
        &mut table_row,
        0x3002_001F,
        &NspiValue::String(
            "/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Address Lists/cn=Global Address List",
        ),
    );

    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    write_u32(&mut body, 1);
    body.push(1);
    write_u32(&mut body, 1);
    body.extend_from_slice(&table_row);
    write_u32(&mut body, 0);
    mapi_response("GetSpecialTable", request_id, 0, body, None)
}

fn nspi_template_info_response(principal: &AccountPrincipal, request_id: &str) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    body.extend_from_slice(&nspi_principal_property_value_list(
        principal,
        NSPI_BOOTSTRAP_PROPERTY_TAGS,
    ));
    write_u32(&mut body, 0);
    mapi_response("GetTemplateInfo", request_id, 0, body, None)
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

fn nspi_resolved_principal_row(principal: &AccountPrincipal, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    row.push(0);
    for property_tag in columns {
        write_address_book_property_value(
            &mut row,
            *property_tag,
            &nspi_principal_value(principal, *property_tag),
        );
    }
    row
}

fn nspi_principal_property_value_list(principal: &AccountPrincipal, tags: &[u32]) -> Vec<u8> {
    let mut values = Vec::new();
    write_u32(&mut values, tags.len() as u32);
    for property_tag in tags {
        write_address_book_tagged_property_value(
            &mut values,
            *property_tag,
            &nspi_principal_value(principal, *property_tag),
        );
    }
    values
}

enum NspiValue<'a> {
    String(&'a str),
    OwnedString(String),
    U32(u32),
}

fn nspi_principal_value(principal: &AccountPrincipal, property_tag: u32) -> NspiValue<'_> {
    match property_tag {
        0x3001_001F => NspiValue::String(&principal.display_name),
        0x39FE_001F => NspiValue::String(&principal.email),
        0x3003_001F => NspiValue::String(&principal.email),
        0x3A00_001F => NspiValue::String(&principal.display_name),
        0x0FFE_0003 => NspiValue::U32(MAPI_MAILUSER_OBJECT_TYPE),
        0x3000_0003 => NspiValue::U32(principal_minimal_entry_id(principal)),
        0x3004_001F => NspiValue::String(&principal.email),
        0x3002_001F => NspiValue::String("SMTP"),
        0x3005_001F => NspiValue::OwnedString(principal_legacy_dn(principal)),
        _ => match property_tag & 0xFFFF {
            0x001F => NspiValue::String(""),
            0x0003 => NspiValue::U32(0),
            _ => NspiValue::U32(0),
        },
    }
}

fn write_large_property_tag_array(body: &mut Vec<u8>, tags: &[u32]) {
    write_u32(body, tags.len() as u32);
    for tag in tags {
        write_u32(body, *tag);
    }
}

fn write_address_book_tagged_property_value(
    body: &mut Vec<u8>,
    property_tag: u32,
    value: &NspiValue<'_>,
) {
    write_u32(body, property_tag);
    write_address_book_property_value(body, property_tag, value);
}

fn write_address_book_property_value(body: &mut Vec<u8>, property_tag: u32, value: &NspiValue<'_>) {
    match (property_tag & 0xFFFF, value) {
        (0x001F, NspiValue::String(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (0x001F, NspiValue::OwnedString(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (0x0003, NspiValue::U32(value)) => write_u32(body, *value),
        (0x0003, _) => write_u32(body, 0),
        (_, NspiValue::U32(value)) => write_u32(body, *value),
        (_, NspiValue::String(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (_, NspiValue::OwnedString(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
    }
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

fn store_session(session_id: String, mut session: MapiSession) {
    let now = SystemTime::now();
    session.last_seen_at = now;
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.insert(session_id, session);
}

fn get_session(session_id: &str) -> Option<MapiSession> {
    let now = SystemTime::now();
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.get(session_id).cloned()
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
    response.extensions_mut().insert(MapiResponseDebug {
        payload_bytes: body.len(),
    });
    let payload_preview_hex = debug_payload_preview_hex(&body);
    if !payload_preview_hex.is_empty() {
        response
            .extensions_mut()
            .insert(MapiResponsePayloadPreview {
                hex: payload_preview_hex,
            });
    }
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

#[derive(Clone, Copy, Debug)]
struct MapiResponseDebug {
    payload_bytes: usize,
}

#[derive(Clone, Debug)]
struct MapiResponsePayloadPreview {
    hex: String,
}

pub(crate) fn mapi_response_payload_bytes(response: &Response) -> Option<usize> {
    response
        .extensions()
        .get::<MapiResponseDebug>()
        .map(|debug| debug.payload_bytes)
}

pub(crate) fn mapi_response_payload_preview_hex(response: &Response) -> Option<&str> {
    response
        .extensions()
        .get::<MapiResponsePayloadPreview>()
        .map(|preview| preview.hex.as_str())
}

fn finalize_mapi_response(mut response: Response, request_headers: &HeaderMap) -> Response {
    insert_header(
        &mut response,
        "x-expirationinfo",
        &(MAPI_SESSION_MAX_AGE_SECONDS * 1000).to_string(),
    );
    insert_header(&mut response, "x-pendingperiod", "15000");
    if let Some(client_info) = request_headers.get("x-clientinfo") {
        response
            .headers_mut()
            .insert("x-clientinfo", client_info.clone());
    }
    response
}

fn log_mapi_connection(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_body: &[u8],
    request_type: &str,
    request_id: &str,
    response: &Response,
) {
    let response_code = response_header(response, "x-responsecode").unwrap_or_default();
    let status = response.status().as_u16();
    let payload_bytes = mapi_response_payload_bytes(response).unwrap_or(0);
    let request_body_bytes = request_body.len();
    let request_body_preview_hex = debug_payload_preview_hex(request_body);
    let response_payload_preview_hex =
        mapi_response_payload_preview_hex(response).unwrap_or_default();
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let content_type = safe_header(headers, "content-type").unwrap_or_default();
    let user_agent = safe_header(headers, "user-agent").unwrap_or_default();
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let message = "rca debug mapi connection";

    if response_code == "0" {
        info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = %request_type,
            mapi_request_id = %request_id,
            client_request_id = %client_request_id,
            client_info = %client_info,
            client_application = %client_application,
            trace_id = %trace_id,
            http_status = status,
            mapi_response_code = %response_code,
            request_body_bytes,
            response_payload_bytes = payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            content_type = %content_type,
            user_agent = %user_agent,
            "{message}"
        );
    } else {
        warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = %request_type,
            mapi_request_id = %request_id,
            client_request_id = %client_request_id,
            client_info = %client_info,
            client_application = %client_application,
            trace_id = %trace_id,
            http_status = status,
            mapi_response_code = %response_code,
            request_body_bytes,
            response_payload_bytes = payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            content_type = %content_type,
            user_agent = %user_agent,
            "{message}"
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

pub(crate) fn safe_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(240).collect())
}

pub(crate) fn debug_payload_preview_hex(bytes: &[u8]) -> String {
    let limit = debug_payload_preview_limit();
    if limit == 0 {
        return String::new();
    }
    hex_preview(bytes, limit)
}

fn debug_payload_preview_limit() -> usize {
    env::var("LPE_RCA_DEBUG_PAYLOAD_PREVIEW_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0)
        .min(512)
}

pub(crate) fn hex_preview(bytes: &[u8], limit: usize) -> String {
    bytes
        .iter()
        .take(limit)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
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

async fn execute_rops<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    rop_buffer: &[u8],
) -> Vec<u8> {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return rop_buffer_with_response(unsupported_rop_response(0, 0), &[]);
    };
    let extended = is_rpc_header_ext_rop_buffer(rop_buffer);
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
                    MapiObject::HierarchyTable {
                        folder_id,
                        columns,
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
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
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
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
                snapshot,
            )),
            0x08 => responses.extend_from_slice(&rop_get_properties_all_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
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
            0x11 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let unread = unread_from_read_flags(request.read_flags());
                let changed = unread.is_some_and(|unread| unread != email.unread);
                if let Some(unread) = unread {
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-set-message-read-flag".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x11,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                }
                responses.extend_from_slice(&rop_set_message_read_flag_response(&request, changed));
            }
            0x12 => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::HierarchyTable { columns, .. })
                    | Some(MapiObject::ContentsTable { columns, .. })
                    | Some(MapiObject::AttachmentTable { columns, .. }) => {
                        *columns = request.property_tags();
                    }
                    _ => {}
                }
                responses.extend_from_slice(&rop_set_columns_response(&request));
            }
            0x13 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::ContentsTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::AttachmentTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                }) => {
                    *sort_orders = request.sort_orders();
                    *position = 0;
                    bookmarks.clear();
                    responses.extend_from_slice(&rop_sort_table_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x13,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x14 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::ContentsTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::AttachmentTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                }) => match request.restriction() {
                    Ok(parsed) => {
                        *restriction = parsed;
                        *position = 0;
                        bookmarks.clear();
                        responses.extend_from_slice(&rop_restrict_response(&request));
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x14,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                },
                _ => responses.extend_from_slice(&rop_error_response(
                    0x14,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x15 => responses.extend_from_slice(&rop_query_rows_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x16 => responses.extend_from_slice(&rop_get_status_response(&request)),
            0x17 => responses.extend_from_slice(&rop_query_position_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x18 => responses.extend_from_slice(&rop_seek_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x19 => responses.extend_from_slice(&rop_seek_row_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x1B => responses.extend_from_slice(&rop_create_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
            )),
            0x4F => responses.extend_from_slice(&rop_find_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x21 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x21,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let row_count = snapshot
                    .attachments_for_message(*folder_id, *message_id)
                    .unwrap_or_default()
                    .len() as u32;
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentTable {
                        folder_id: *folder_id,
                        message_id: *message_id,
                        columns: Vec::new(),
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses
                    .extend_from_slice(&rop_get_attachment_table_response(&request, row_count));
                output_handles.push(handle);
            }
            0x22 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                if snapshot
                    .attachment_for_message(*folder_id, *message_id, attach_num)
                    .is_some()
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Attachment {
                            folder_id: *folder_id,
                            message_id: *message_id,
                            attach_num,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_attachment_response(&request));
                    output_handles.push(handle);
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            0x2B => {
                let Some(MapiObject::Attachment {
                    folder_id,
                    message_id,
                    attach_num,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                if request.stream_property_tag() != Some(PID_TAG_ATTACH_DATA_BINARY) {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    ));
                    continue;
                }
                let Some(attachment) =
                    snapshot.attachment_for_message(*folder_id, *message_id, *attach_num)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let content = store
                    .fetch_attachment_content(principal.account_id, &attachment.file_reference)
                    .await;
                let Ok(Some(content)) = content else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let stream_size = content.blob_bytes.len();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentStream {
                        data: content.blob_bytes,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_open_stream_response(&request, stream_size));
                output_handles.push(handle);
            }
            0x2C => {
                let Some(stream) = input_object_mut(session, &handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                responses.extend_from_slice(&rop_read_stream_response(&request, stream));
            }
            0x27 => responses.extend_from_slice(&rop_get_receive_folder_response(&request)),
            0x66 => {
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x66,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let unread = unread_from_read_flags(request.read_flags());
                let mut partial_completion = false;
                for message_id in request.message_ids() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if let Some(unread) = unread {
                        if store
                            .update_jmap_email_flags(
                                principal.account_id,
                                email.id,
                                Some(unread),
                                None,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-set-read-flags".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                    }
                }
                responses
                    .extend_from_slice(&rop_set_read_flags_response(&request, partial_completion));
            }
            0x68 => responses.extend_from_slice(&rop_get_receive_folder_table_response(&request)),
            0x7B => responses.extend_from_slice(&rop_get_store_state_response(&request)),
            0x81 => {
                if let Some(table) = input_object_mut(session, &handle_slots, &request) {
                    reset_table_position(table);
                }
                responses.extend_from_slice(&rop_reset_table_response(&request));
            }
            0x89 => responses.extend_from_slice(&rop_free_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
            )),
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
    let response = if extended {
        rop_buffer_with_response_spec(responses, &output_handles)
    } else {
        rop_buffer_with_response(responses, &output_handles)
    };
    if extended {
        rpc_header_ext_rop_buffer(response)
    } else {
        response
    }
}

const STORE_REPLICA_ID: u64 = 1;
const ROOT_FOLDER_ID: u64 = mapi_store_id(1);
const DEFERRED_ACTION_FOLDER_ID: u64 = mapi_store_id(2);
const SPOOLER_QUEUE_FOLDER_ID: u64 = mapi_store_id(3);
const IPM_SUBTREE_FOLDER_ID: u64 = mapi_store_id(4);
const INBOX_FOLDER_ID: u64 = mapi_store_id(5);
const OUTBOX_FOLDER_ID: u64 = mapi_store_id(6);
const SENT_FOLDER_ID: u64 = mapi_store_id(7);
const TRASH_FOLDER_ID: u64 = mapi_store_id(8);
const COMMON_VIEWS_FOLDER_ID: u64 = mapi_store_id(9);
const SCHEDULE_FOLDER_ID: u64 = mapi_store_id(10);
const SEARCH_FOLDER_ID: u64 = mapi_store_id(11);
const VIEWS_FOLDER_ID: u64 = mapi_store_id(12);
const SHORTCUTS_FOLDER_ID: u64 = mapi_store_id(13);
const DRAFTS_FOLDER_ID: u64 = mapi_store_id(14);

const fn mapi_store_id(global_counter: u64) -> u64 {
    ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | STORE_REPLICA_ID
}

const PRIVATE_LOGON_SPECIAL_FOLDER_IDS: [u64; 13] = [
    ROOT_FOLDER_ID,
    DEFERRED_ACTION_FOLDER_ID,
    SPOOLER_QUEUE_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID,
    INBOX_FOLDER_ID,
    OUTBOX_FOLDER_ID,
    SENT_FOLDER_ID,
    TRASH_FOLDER_ID,
    COMMON_VIEWS_FOLDER_ID,
    SCHEDULE_FOLDER_ID,
    SEARCH_FOLDER_ID,
    VIEWS_FOLDER_ID,
    SHORTCUTS_FOLDER_ID,
];

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
const PID_TAG_ATTACH_DATA_BINARY: u32 = 0x3701_0102;
const PID_TAG_ATTACH_SIZE: u32 = 0x0E20_0003;
const PID_TAG_ATTACH_NUM: u32 = 0x0E21_0003;
const PID_TAG_ATTACH_FILENAME_W: u32 = 0x3704_001F;
const PID_TAG_ATTACH_METHOD: u32 = 0x3705_0003;
const PID_TAG_ATTACH_LONG_FILENAME_W: u32 = 0x3707_001F;
const PID_TAG_RENDERING_POSITION: u32 = 0x370B_0003;
const PID_TAG_ATTACH_MIME_TAG_W: u32 = 0x370E_001F;

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
            MapiObject::AttachmentStream { .. } => None,
            MapiObject::Logon => Some(ROOT_FOLDER_ID),
            MapiObject::Folder { folder_id }
            | MapiObject::Message { folder_id, .. }
            | MapiObject::HierarchyTable { folder_id, .. }
            | MapiObject::ContentsTable { folder_id, .. }
            | MapiObject::AttachmentTable { folder_id, .. }
            | MapiObject::Attachment { folder_id, .. } => Some(*folder_id),
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

fn reset_table_position(object: &mut MapiObject) {
    match object {
        MapiObject::HierarchyTable {
            position,
            bookmarks,
            ..
        }
        | MapiObject::ContentsTable {
            position,
            bookmarks,
            ..
        }
        | MapiObject::AttachmentTable {
            position,
            bookmarks,
            ..
        } => {
            *position = 0;
            bookmarks.clear();
        }
        _ => {}
    }
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

fn default_attachment_columns() -> Vec<u32> {
    vec![
        PID_TAG_ATTACH_NUM,
        PID_TAG_ATTACH_LONG_FILENAME_W,
        PID_TAG_ATTACH_FILENAME_W,
        PID_TAG_ATTACH_MIME_TAG_W,
        PID_TAG_ATTACH_SIZE,
        PID_TAG_ATTACH_METHOD,
        PID_TAG_RENDERING_POSITION,
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
    if let Some(payload) = rpc_header_ext_payload(buffer) {
        return split_rop_payload_spec(payload);
    }
    split_rop_payload_legacy(buffer)
}

fn split_rop_payload_spec(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    if buffer.len() < 2 {
        return None;
    }
    let rop_size = u16::from_le_bytes([buffer[0], buffer[1]]) as usize;
    if rop_size < 2 || buffer.len() < rop_size {
        return None;
    }
    Some((&buffer[2..rop_size], &buffer[rop_size..]))
}

fn split_rop_payload_legacy(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    if buffer.len() < 2 {
        return None;
    }
    let rop_size = u16::from_le_bytes([buffer[0], buffer[1]]) as usize;
    if buffer.len() < 2 + rop_size {
        return None;
    }
    Some((&buffer[2..2 + rop_size], &buffer[2 + rop_size..]))
}

fn is_rpc_header_ext_rop_buffer(buffer: &[u8]) -> bool {
    rpc_header_ext_payload(buffer).is_some()
}

fn rpc_header_ext_payload(buffer: &[u8]) -> Option<&[u8]> {
    if buffer.len() < 10 {
        return None;
    }
    let version = u16::from_le_bytes([buffer[0], buffer[1]]);
    let flags = u16::from_le_bytes([buffer[2], buffer[3]]);
    let size = u16::from_le_bytes([buffer[4], buffer[5]]) as usize;
    let size_actual = u16::from_le_bytes([buffer[6], buffer[7]]) as usize;
    if version != 0 || size == 0 || size > size_actual || buffer.len() < 8 + size {
        return None;
    }
    // The RCA bootstrap uses an uncompressed, unobfuscated RPC_HEADER_EXT payload
    // with the Last flag. Compression and XOR obfuscation are handled later.
    if flags & !0x0004 != 0 {
        return None;
    }
    let payload = &buffer[8..8 + size];
    split_rop_payload_spec(payload)?;
    Some(payload)
}

fn rpc_header_ext_rop_buffer(payload: Vec<u8>) -> Vec<u8> {
    let size = payload.len().min(u16::MAX as usize) as u16;
    let mut buffer = Vec::with_capacity(8 + payload.len());
    buffer.extend_from_slice(&0u16.to_le_bytes());
    buffer.extend_from_slice(&0x0004u16.to_le_bytes());
    buffer.extend_from_slice(&size.to_le_bytes());
    buffer.extend_from_slice(&size.to_le_bytes());
    buffer.extend_from_slice(&payload);
    buffer
}

fn rop_logon_response_body(principal: &AccountPrincipal, request: &RopRequest) -> Vec<u8> {
    let output_handle_index = request.output_handle_index.unwrap_or(0);
    let logon_flags = request.payload.first().copied().unwrap_or(0x01) & 0x07 | 0x01;
    let mut response = Vec::new();
    response.push(0xFE);
    response.push(output_handle_index);
    write_u32(&mut response, 0);
    response.push(logon_flags);
    for folder_id in PRIVATE_LOGON_SPECIAL_FOLDER_IDS {
        write_u64(&mut response, folder_id);
    }
    response.push(0x03);
    response.extend_from_slice(principal.account_id.as_bytes());
    response.extend_from_slice(&1u16.to_le_bytes());
    response.extend_from_slice(principal.account_id.as_bytes());
    let now = SystemTime::now();
    response.extend_from_slice(&logon_time_bytes(now));
    write_u64(&mut response, gwart_time_marker(now));
    write_u32(&mut response, 0);
    response
}

fn gwart_time_marker(now: SystemTime) -> u64 {
    now.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
        .max(1)
}

fn logon_time_bytes(now: SystemTime) -> [u8; 8] {
    let duration = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    let seconds = duration.as_secs();
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = seconds % 86_400;
    let hour = (seconds_of_day / 3_600) as u8;
    let minute = ((seconds_of_day % 3_600) / 60) as u8;
    let second = (seconds_of_day % 60) as u8;
    let day_of_week = ((days + 4).rem_euclid(7)) as u8;
    let (year, month, day) = civil_from_unix_days(days);
    let year = (year as u16).to_le_bytes();
    [
        second,
        minute,
        hour,
        day_of_week,
        day,
        month,
        year[0],
        year[1],
    ]
}

fn civil_from_unix_days(days: i64) -> (i32, u8, u8) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u8, day as u8)
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

fn rop_get_attachment_table_response(request: &RopRequest, row_count: u32) -> Vec<u8> {
    let mut response = vec![0x21, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u32(&mut response, row_count);
    response
}

fn rop_open_attachment_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x22, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

fn rop_open_stream_response(request: &RopRequest, stream_size: usize) -> Vec<u8> {
    let mut response = vec![0x2B, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u64(&mut response, stream_size as u64);
    response
}

fn rop_read_stream_response(request: &RopRequest, stream: &mut MapiObject) -> Vec<u8> {
    let input_handle_index = request.input_handle_index().unwrap_or(0);
    let MapiObject::AttachmentStream { data, position } = stream else {
        return rop_error_response(0x2C, input_handle_index, 0x8004_010F);
    };
    let requested = request
        .read_byte_count()
        .map(usize::from)
        .unwrap_or(0)
        .min(u16::MAX as usize);
    let end = position.saturating_add(requested).min(data.len());
    let chunk = data[*position..end].to_vec();
    *position = end;

    let mut response = vec![0x2C, input_handle_index];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(chunk.len() as u16).to_le_bytes());
    response.extend_from_slice(&chunk);
    response
}

fn rop_set_read_flags_response(request: &RopRequest, partial_completion: bool) -> Vec<u8> {
    let mut response = vec![0x66, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(partial_completion as u8);
    response
}

fn rop_set_columns_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x12, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

fn rop_sort_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x13, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

fn rop_restrict_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x14, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

fn rop_get_properties_list_response(request: &RopRequest, object: Option<&MapiObject>) -> Vec<u8> {
    let tags = match object {
        Some(MapiObject::Attachment { .. }) => default_attachment_columns(),
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
    snapshot: &MapiMailStoreSnapshot,
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
        Some(MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        }) => {
            let Some(attachment) =
                snapshot.attachment_for_message(*folder_id, *message_id, *attach_num)
            else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_attachment_row(attachment, &columns)
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

fn rop_get_properties_all_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let mut response = vec![0x08, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let tags = match object {
        Some(MapiObject::Attachment { .. }) => default_attachment_columns(),
        Some(MapiObject::Message { .. }) => default_message_property_tags(),
        _ => default_folder_property_tags(),
    };
    response.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        write_u32(&mut response, tag);
        let value = serialize_object_property(object, mailboxes, emails, snapshot, tag);
        response.extend_from_slice(&value);
    }
    response
}

fn serialize_object_property(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    tag: u32,
) -> Vec<u8> {
    match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
        }) => message_for_id(*folder_id, *message_id, mailboxes, emails)
            .map(|email| serialize_message_row(email, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        }) => snapshot
            .attachment_for_message(*folder_id, *message_id, *attach_num)
            .map(|attachment| serialize_attachment_row(attachment, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| serialize_folder_row(mailbox, &[tag]))
                .unwrap_or_else(|| serialize_root_folder_row(mailboxes, &[tag]))
        }
    }
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

fn rop_set_message_read_flag_response(request: &RopRequest, read_status_changed: bool) -> Vec<u8> {
    let mut response = vec![0x11, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(read_status_changed as u8);
    response
}

fn rop_query_rows_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let mut response = vec![0x15, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0x02);
    let mut start_position = 0usize;
    let rows = match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            columns,
            sort_orders,
            restriction,
            position: table_position,
            ..
        }) if is_root_hierarchy_folder(*folder_id) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            let mut rows = mailboxes.iter().collect::<Vec<_>>();
            rows.retain(|mailbox| restriction_matches_mailbox(restriction.as_ref(), mailbox));
            sort_mailboxes(&mut rows, sort_orders);
            rows.into_iter()
                .map(|mailbox| serialize_folder_row(mailbox, &columns))
                .collect::<Vec<_>>()
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            columns,
            sort_orders,
            restriction,
            position: table_position,
            ..
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_contents_columns()
            } else {
                columns.clone()
            };
            let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
            rows.retain(|email| restriction_matches_email(restriction.as_ref(), email));
            sort_emails(&mut rows, sort_orders);
            rows.into_iter()
                .map(|email| serialize_message_row(email, &columns))
                .collect::<Vec<_>>()
        }
        Some(MapiObject::AttachmentTable {
            folder_id,
            message_id,
            columns,
            sort_orders,
            restriction,
            position: table_position,
            ..
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_attachment_columns()
            } else {
                columns.clone()
            };
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            rows.retain(|attachment| {
                restriction_matches_attachment(restriction.as_ref(), attachment)
            });
            sort_attachments(&mut rows, sort_orders);
            rows.into_iter()
                .map(|attachment| serialize_attachment_row(attachment, &columns))
                .collect::<Vec<_>>()
        }
        _ => Vec::new(),
    };
    let row_count = request.query_row_count().unwrap_or(rows.len());
    let forward_read = request.query_forward_read();
    let (selected, next_position) = if forward_read {
        let selected = rows
            .into_iter()
            .skip(start_position)
            .take(row_count)
            .collect::<Vec<_>>();
        let next_position = start_position.saturating_add(selected.len());
        (selected, next_position)
    } else {
        let end_position = start_position.min(rows.len());
        let selected_start = end_position.saturating_sub(row_count);
        let selected = rows[selected_start..end_position]
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        (selected, selected_start)
    };
    if !request.query_no_advance() {
        if let Some(
            MapiObject::HierarchyTable { position, .. }
            | MapiObject::ContentsTable { position, .. }
            | MapiObject::AttachmentTable { position, .. },
        ) = object
        {
            *position = next_position;
        }
    }
    response.extend_from_slice(&(selected.len() as u16).to_le_bytes());
    for row in selected {
        response.extend_from_slice(&row);
    }
    response
}

fn sort_mailboxes(rows: &mut [&JmapMailbox], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_DISPLAY_NAME_W => compare_case_insensitive(&left.name, &right.name),
                PID_TAG_CONTENT_COUNT => left.total_emails.cmp(&right.total_emails),
                PID_TAG_CONTENT_UNREAD_COUNT => left.unread_emails.cmp(&right.unread_emails),
                PID_TAG_FOLDER_ID => mapi_folder_id(left).cmp(&mapi_folder_id(right)),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

fn sort_emails(rows: &mut [&JmapEmail], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.subject, &right.subject)
                }
                PID_TAG_SENDER_NAME_W => compare_case_insensitive(
                    left.from_display.as_deref().unwrap_or(&left.from_address),
                    right.from_display.as_deref().unwrap_or(&right.from_address),
                ),
                PID_TAG_SENDER_EMAIL_ADDRESS_W => {
                    compare_case_insensitive(&left.from_address, &right.from_address)
                }
                PID_TAG_DISPLAY_TO_W => {
                    compare_case_insensitive(&display_to(left), &display_to(right))
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.received_at.cmp(&right.received_at)
                }
                PID_TAG_MESSAGE_FLAGS => message_flags(left).cmp(&message_flags(right)),
                PID_TAG_MESSAGE_SIZE => left.size_octets.cmp(&right.size_octets),
                PID_TAG_HAS_ATTACHMENTS => left.has_attachments.cmp(&right.has_attachments),
                PID_TAG_MID => mapi_message_id(left).cmp(&mapi_message_id(right)),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

fn sort_attachments(rows: &mut [&MapiAttachment], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_ATTACH_NUM => left.attach_num.cmp(&right.attach_num),
                PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                    compare_case_insensitive(&left.file_name, &right.file_name)
                }
                PID_TAG_ATTACH_MIME_TAG_W => {
                    compare_case_insensitive(&left.media_type, &right.media_type)
                }
                PID_TAG_ATTACH_SIZE => left.size_octets.cmp(&right.size_octets),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

fn apply_sort_direction(ordering: Ordering, sort_order: u8) -> Ordering {
    if sort_order == 0x01 {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_case_insensitive(left: &str, right: &str) -> Ordering {
    left.to_ascii_lowercase().cmp(&right.to_ascii_lowercase())
}

fn restriction_matches_mailbox(
    restriction: Option<&MapiRestriction>,
    mailbox: &JmapMailbox,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        mailbox_property_value(mailbox, property_tag)
    })
}

fn restriction_matches_email(restriction: Option<&MapiRestriction>, email: &JmapEmail) -> bool {
    restriction_matches(restriction, |property_tag| {
        email_property_value(email, property_tag)
    })
}

fn restriction_matches_attachment(
    restriction: Option<&MapiRestriction>,
    attachment: &MapiAttachment,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        attachment_property_value(attachment, property_tag)
    })
}

fn restriction_matches(
    restriction: Option<&MapiRestriction>,
    value_for: impl Copy + Fn(u32) -> Option<MapiValue>,
) -> bool {
    let Some(restriction) = restriction else {
        return true;
    };
    match restriction {
        MapiRestriction::And(children) => children
            .iter()
            .all(|child| restriction_matches(Some(child), value_for)),
        MapiRestriction::Or(children) => children
            .iter()
            .any(|child| restriction_matches(Some(child), value_for)),
        MapiRestriction::Not(child) => !restriction_matches(Some(child), value_for),
        MapiRestriction::Content {
            property_tag,
            value,
        } => value_for(*property_tag)
            .and_then(|property| property.into_text())
            .is_some_and(|property| {
                property
                    .to_ascii_lowercase()
                    .contains(&value.to_ascii_lowercase())
            }),
        MapiRestriction::Property {
            relop,
            property_tag,
            value,
        } => value_for(*property_tag)
            .is_some_and(|property| compare_mapi_values(&property, value, *relop)),
        MapiRestriction::Bitmask {
            property_tag,
            mask,
            must_be_nonzero,
        } => value_for(*property_tag)
            .and_then(|value| value.into_u32())
            .is_some_and(|value| ((value & mask) != 0) == *must_be_nonzero),
        MapiRestriction::Size {
            relop,
            property_tag,
            size,
        } => value_for(*property_tag)
            .map(|value| value.size() as i64)
            .is_some_and(|actual| compare_i64(actual, *size as i64, *relop)),
        MapiRestriction::Exist { property_tag } => value_for(*property_tag).is_some(),
    }
}

fn mailbox_property_value(mailbox: &JmapMailbox, property_tag: u32) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(mailbox.name.clone())),
        PID_TAG_CONTENT_COUNT => Some(MapiValue::U32(mailbox.total_emails)),
        PID_TAG_CONTENT_UNREAD_COUNT => Some(MapiValue::U32(mailbox.unread_emails)),
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(false)),
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(mapi_folder_id(mailbox))),
        _ => None,
    }
}

fn email_property_value(email: &JmapEmail, property_tag: u32) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(mapi_message_id(email))),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(email.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Note".to_string())),
        PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
            Some(MapiValue::String(email.received_at.clone()))
        }
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(message_flags(email))),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(email.size_octets)),
        PID_TAG_SENDER_NAME_W => Some(MapiValue::String(
            email
                .from_display
                .clone()
                .unwrap_or_else(|| email.from_address.clone()),
        )),
        PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(email.from_address.clone())),
        PID_TAG_DISPLAY_TO_W => Some(MapiValue::String(display_to(email))),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(email.has_attachments)),
        PID_TAG_BODY_W => Some(MapiValue::String(email.body_text.clone())),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
            Some(MapiValue::Binary(email.id.as_bytes().to_vec()))
        }
        PID_TAG_INTERNET_MESSAGE_ID_W => email.internet_message_id.clone().map(MapiValue::String),
        _ => None,
    }
}

fn attachment_property_value(attachment: &MapiAttachment, property_tag: u32) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_ATTACH_NUM => Some(MapiValue::U32(attachment.attach_num)),
        PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
            Some(MapiValue::String(attachment.file_name.clone()))
        }
        PID_TAG_ATTACH_MIME_TAG_W => Some(MapiValue::String(attachment.media_type.clone())),
        PID_TAG_ATTACH_SIZE => Some(MapiValue::U64(attachment.size_octets)),
        PID_TAG_ATTACH_METHOD => Some(MapiValue::U32(1)),
        PID_TAG_RENDERING_POSITION => Some(MapiValue::U32(u32::MAX)),
        PID_TAG_ENTRY_ID => Some(MapiValue::Binary(
            attachment.canonical_id.as_bytes().to_vec(),
        )),
        PID_TAG_INSTANCE_KEY => Some(MapiValue::String(attachment.file_reference.clone())),
        _ => None,
    }
}

fn compare_mapi_values(left: &MapiValue, right: &MapiValue, relop: u8) -> bool {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return compare_i64(left, right, relop);
    }
    if let (Some(left), Some(right)) = (left.as_text(), right.as_text()) {
        return compare_ordering(compare_case_insensitive(left, right), relop);
    }
    if let (Some(left), Some(right)) = (left.as_bool(), right.as_bool()) {
        return compare_ordering(left.cmp(&right), relop);
    }
    compare_ordering(left.cmp_value(right), relop)
}

fn compare_i64(left: i64, right: i64, relop: u8) -> bool {
    compare_ordering(left.cmp(&right), relop)
}

fn compare_ordering(ordering: Ordering, relop: u8) -> bool {
    match relop {
        0x00 => ordering == Ordering::Less,
        0x01 => matches!(ordering, Ordering::Less | Ordering::Equal),
        0x02 => ordering == Ordering::Greater,
        0x03 => matches!(ordering, Ordering::Greater | Ordering::Equal),
        0x04 => ordering == Ordering::Equal,
        0x05 => ordering != Ordering::Equal,
        _ => false,
    }
}

impl MapiValue {
    fn as_i64(&self) -> Option<i64> {
        match self {
            MapiValue::Bool(value) => Some(i64::from(*value)),
            MapiValue::I32(value) => Some(i64::from(*value)),
            MapiValue::I64(value) => Some(*value),
            MapiValue::U32(value) => Some(i64::from(*value)),
            MapiValue::U64(value) => i64::try_from(*value).ok(),
            MapiValue::String(_) | MapiValue::Binary(_) => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            MapiValue::Bool(value) => Some(*value),
            MapiValue::I32(value) => Some(*value != 0),
            MapiValue::I64(value) => Some(*value != 0),
            MapiValue::U32(value) => Some(*value != 0),
            MapiValue::U64(value) => Some(*value != 0),
            MapiValue::String(_) | MapiValue::Binary(_) => None,
        }
    }

    fn as_text(&self) -> Option<&str> {
        match self {
            MapiValue::String(value) => Some(value),
            _ => None,
        }
    }

    fn into_text(self) -> Option<String> {
        match self {
            MapiValue::Bool(value) => Some(value.to_string()),
            MapiValue::I32(value) => Some(value.to_string()),
            MapiValue::I64(value) => Some(value.to_string()),
            MapiValue::U32(value) => Some(value.to_string()),
            MapiValue::U64(value) => Some(value.to_string()),
            MapiValue::String(value) => Some(value),
            MapiValue::Binary(_) => None,
        }
    }

    fn into_u32(self) -> Option<u32> {
        match self {
            MapiValue::Bool(value) => Some(u32::from(value)),
            MapiValue::I32(value) => u32::try_from(value).ok(),
            MapiValue::I64(value) => u32::try_from(value).ok(),
            MapiValue::U32(value) => Some(value),
            MapiValue::U64(value) => u32::try_from(value).ok(),
            MapiValue::String(_) | MapiValue::Binary(_) => None,
        }
    }

    fn size(&self) -> usize {
        match self {
            MapiValue::Bool(_) => 1,
            MapiValue::I32(_) | MapiValue::U32(_) => 4,
            MapiValue::I64(_) | MapiValue::U64(_) => 8,
            MapiValue::String(value) => value.encode_utf16().count() * 2,
            MapiValue::Binary(value) => value.len(),
        }
    }

    fn cmp_value(&self, other: &MapiValue) -> Ordering {
        format!("{self:?}").cmp(&format!("{other:?}"))
    }
}

fn serialize_attachment_row(attachment: &MapiAttachment, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attachment.attach_num),
            PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &attachment.file_name)
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &attachment.media_type),
            PID_TAG_ATTACH_SIZE => {
                write_u32(&mut row, attachment.size_octets.min(u32::MAX as u64) as u32)
            }
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, 1),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ENTRY_ID => {
                write_u16_prefixed_bytes(&mut row, attachment.canonical_id.as_bytes())
            }
            PID_TAG_INSTANCE_KEY => {
                write_u16_prefixed_bytes(&mut row, attachment.file_reference.as_bytes())
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn rop_get_status_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x16, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

fn rop_query_position_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let (position, row_count) = table_position_and_count(object, mailboxes, emails, snapshot);
    let mut response = vec![0x17, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, position as u32);
    write_u32(&mut response, row_count as u32);
    response
}

fn rop_seek_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };
    let total_rows = table_position_and_count(Some(object), mailboxes, emails, snapshot).1;
    let Some(position) = table_position_mut(object) else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };

    let requested_rows = request.seek_row_count().unwrap_or(0);
    let base_position = match request.seek_origin().unwrap_or(1) {
        0 => 0isize,
        2 => total_rows as isize,
        _ => *position as isize,
    };
    let requested_position = base_position.saturating_add(requested_rows as isize);
    let new_position = requested_position.clamp(0, total_rows as isize);
    let rows_sought = (new_position - base_position) as i32;
    *position = new_position as usize;

    let mut response = vec![0x18, request.response_handle_index()];
    write_u32(&mut response, 0);
    let want_row_moved_count = request.want_row_moved_count();
    response.push((want_row_moved_count && rows_sought != requested_rows) as u8);
    response.extend_from_slice(&if want_row_moved_count { rows_sought } else { 0 }.to_le_bytes());
    response
}

fn rop_create_bookmark_response(request: &RopRequest, object: Option<&mut MapiObject>) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x1B, request.response_handle_index(), 0x8004_0102);
    };
    let Some((position, bookmarks, next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x1B, request.response_handle_index(), 0x8004_0102);
    };
    let bookmark = next_bookmark.to_le_bytes().to_vec();
    bookmarks.insert(bookmark.clone(), *position);
    *next_bookmark = next_bookmark.saturating_add(1).max(1);

    let mut response = vec![0x1B, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    response.extend_from_slice(&bookmark);
    response
}

fn rop_seek_row_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0102);
    };
    let total_rows = table_position_and_count(Some(object), mailboxes, emails, snapshot).1;
    let Some((position, bookmarks, _next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0102);
    };
    let Some(base_position) = bookmarks.get(request.bookmark()).copied() else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0405);
    };

    let requested_rows = request.bookmark_row_count().unwrap_or(0);
    let requested_position = (base_position as isize).saturating_add(requested_rows as isize);
    let new_position = requested_position.clamp(0, total_rows as isize);
    let rows_sought = (new_position - base_position as isize) as i32;
    *position = new_position as usize;

    let mut response = vec![0x19, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response.push((request.bookmark_want_row_moved_count() && rows_sought != requested_rows) as u8);
    response.extend_from_slice(
        &if request.bookmark_want_row_moved_count() {
            rows_sought
        } else {
            0
        }
        .to_le_bytes(),
    );
    response
}

fn rop_free_bookmark_response(request: &RopRequest, object: Option<&mut MapiObject>) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    let Some((_position, bookmarks, _next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    bookmarks.remove(request.bookmark());

    let mut response = vec![0x89, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

fn rop_find_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Ok(restriction) = request.restriction() else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    let Some(restriction) = restriction else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };

    let Some(object) = object else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    let mut response = vec![0x4F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);

    match object {
        MapiObject::HierarchyTable {
            folder_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } if is_root_hierarchy_folder(*folder_id) => {
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            let mut rows = mailboxes.iter().collect::<Vec<_>>();
            rows.retain(|mailbox| restriction_matches_mailbox(table_restriction.as_ref(), mailbox));
            sort_mailboxes(&mut rows, sort_orders);
            if let Some((index, mailbox)) =
                find_row(rows.as_slice(), *position, request, |mailbox| {
                    restriction_matches_mailbox(Some(&restriction), mailbox)
                })
            {
                *position = index;
                response.push(1);
                response.extend_from_slice(&serialize_folder_row(mailbox, &columns));
            } else {
                response.push(0);
            }
        }
        MapiObject::ContentsTable {
            folder_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } => {
            let columns = if columns.is_empty() {
                default_contents_columns()
            } else {
                columns.clone()
            };
            let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
            rows.retain(|email| restriction_matches_email(table_restriction.as_ref(), email));
            sort_emails(&mut rows, sort_orders);
            if let Some((index, email)) = find_row(rows.as_slice(), *position, request, |email| {
                restriction_matches_email(Some(&restriction), email)
            }) {
                *position = index;
                response.push(1);
                response.extend_from_slice(&serialize_message_row(email, &columns));
            } else {
                response.push(0);
            }
        }
        MapiObject::AttachmentTable {
            folder_id,
            message_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } => {
            let columns = if columns.is_empty() {
                default_attachment_columns()
            } else {
                columns.clone()
            };
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            rows.retain(|attachment| {
                restriction_matches_attachment(table_restriction.as_ref(), attachment)
            });
            sort_attachments(&mut rows, sort_orders);
            if let Some((index, attachment)) =
                find_row(rows.as_slice(), *position, request, |attachment| {
                    restriction_matches_attachment(Some(&restriction), attachment)
                })
            {
                *position = index;
                response.push(1);
                response.extend_from_slice(&serialize_attachment_row(attachment, &columns));
            } else {
                response.push(0);
            }
        }
        _ => return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102),
    }

    response
}

fn find_row<'a, T>(
    rows: &'a [&'a T],
    current_position: usize,
    request: &RopRequest,
    matches: impl Fn(&T) -> bool,
) -> Option<(usize, &'a T)> {
    if rows.is_empty() {
        return None;
    }
    let start = match request.find_origin().unwrap_or(1) {
        0 => 0,
        2 => rows.len().saturating_sub(1),
        _ => current_position.min(rows.len()),
    };
    if request.find_backward() {
        let end = start.min(rows.len().saturating_sub(1));
        (0..=end)
            .rev()
            .find_map(|index| matches(rows[index]).then_some((index, rows[index])))
    } else {
        rows.iter()
            .enumerate()
            .skip(start)
            .find_map(|(index, row)| matches(row).then_some((index, *row)))
    }
}

fn table_position_and_count(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> (usize, usize) {
    match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            position,
            restriction,
            ..
        }) if is_root_hierarchy_folder(*folder_id) => (
            *position,
            mailboxes
                .iter()
                .filter(|mailbox| restriction_matches_mailbox(restriction.as_ref(), mailbox))
                .count(),
        ),
        Some(MapiObject::ContentsTable {
            folder_id,
            position,
            restriction,
            ..
        }) => (
            *position,
            emails_for_folder(*folder_id, mailboxes, emails)
                .into_iter()
                .filter(|email| restriction_matches_email(restriction.as_ref(), email))
                .count(),
        ),
        Some(MapiObject::AttachmentTable {
            folder_id,
            message_id,
            position,
            restriction,
            ..
        }) => (
            *position,
            snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .filter(|attachment| {
                    restriction_matches_attachment(restriction.as_ref(), attachment)
                })
                .count(),
        ),
        _ => (0, 0),
    }
}

fn table_position_mut(object: &mut MapiObject) -> Option<&mut usize> {
    match object {
        MapiObject::HierarchyTable { position, .. }
        | MapiObject::ContentsTable { position, .. }
        | MapiObject::AttachmentTable { position, .. } => Some(position),
        _ => None,
    }
}

fn table_bookmark_state_mut(
    object: &mut MapiObject,
) -> Option<(&mut usize, &mut HashMap<Vec<u8>, usize>, &mut u32)> {
    match object {
        MapiObject::HierarchyTable {
            position,
            bookmarks,
            next_bookmark,
            ..
        }
        | MapiObject::ContentsTable {
            position,
            bookmarks,
            next_bookmark,
            ..
        } => Some((position, bookmarks, next_bookmark)),
        _ => None,
    }
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
        OUTBOX_FOLDER_ID => Some("outbox"),
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
        .chain(
            message_can_expose_bcc(email)
                .then_some(email.bcc.iter())
                .into_iter()
                .flatten()
                .map(|address| MapiRecipient {
                    recipient_type: 0x03,
                    address,
                }),
        )
        .collect()
}

fn message_can_expose_bcc(email: &JmapEmail) -> bool {
    matches!(email.mailbox_role.as_str(), "drafts" | "sent")
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

fn unread_from_read_flags(read_flags: Option<u8>) -> Option<bool> {
    match read_flags {
        Some(flags) if flags & 0x10 != 0 => None,
        Some(flags) if flags & 0x04 != 0 => Some(true),
        Some(_) => Some(false),
        None => Some(false),
    }
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
    match mailbox.role.as_str() {
        "inbox" => INBOX_FOLDER_ID,
        "drafts" => DRAFTS_FOLDER_ID,
        "outbox" => OUTBOX_FOLDER_ID,
        "sent" => SENT_FOLDER_ID,
        "trash" => TRASH_FOLDER_ID,
        _ => mapi_store_id(uuid_global_counter(&mailbox.id)),
    }
}

fn mapi_message_id(email: &JmapEmail) -> u64 {
    mapi_store_id(uuid_global_counter(&email.id))
}

fn uuid_global_counter(id: &Uuid) -> u64 {
    let bytes = id.as_bytes();
    let value = u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]) & 0x0000_FFFF_FFFF_FFFF;
    value.max(0x100)
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

fn rop_buffer_with_response_spec(response: Vec<u8>, output_handles: &[u32]) -> Vec<u8> {
    let mut buffer = Vec::new();
    let rop_size = response.len().saturating_add(2).min(u16::MAX as usize) as u16;
    buffer.extend_from_slice(&rop_size.to_le_bytes());
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

    fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
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

    fn read_ascii_z(&mut self) -> Result<String> {
        let start = self.position;
        while self.remaining() > 0 {
            if self.bytes[self.position] == 0 {
                let bytes = &self.bytes[start..self.position];
                self.position += 1;
                return Ok(String::from_utf8_lossy(bytes).into_owned());
            }
            self.position += 1;
        }
        Err(anyhow!("unterminated ASCII string"))
    }

    fn read_utf16z(&mut self) -> Result<String> {
        let mut units = Vec::new();
        loop {
            let unit = self.read_u16()?;
            if unit == 0 {
                return String::from_utf16(&units)
                    .map_err(|_| anyhow!("invalid UTF-16 string in restriction"));
            }
            units.push(unit);
        }
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
        if self.rop_id == 0x11 {
            return self.output_handle_index.unwrap_or(0);
        }
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

    fn attach_num(&self) -> Option<u32> {
        let bytes = self.payload.get(1..5)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    fn stream_property_tag(&self) -> Option<u32> {
        let bytes = self.payload.get(..4)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    fn read_byte_count(&self) -> Option<u16> {
        let bytes = self.payload.get(..2)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?))
    }

    fn read_flags(&self) -> Option<u8> {
        match self.rop_id {
            0x11 => self.payload.first().copied(),
            0x66 => self.payload.get(1).copied(),
            _ => None,
        }
    }

    fn message_ids(&self) -> Vec<u64> {
        let Some(count_bytes) = self.payload.get(2..4) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload[4..]
            .chunks_exact(8)
            .take(count)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or_default()))
            .collect()
    }

    fn query_row_count(&self) -> Option<usize> {
        let bytes = self.payload.get(2..4)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?) as usize)
    }

    fn query_no_advance(&self) -> bool {
        self.payload.first().is_some_and(|flags| flags & 0x01 != 0)
    }

    fn query_forward_read(&self) -> bool {
        self.payload
            .get(1)
            .map(|forward| *forward != 0)
            .unwrap_or(true)
    }

    fn restriction(&self) -> Result<Option<MapiRestriction>> {
        let Some(size_bytes) = self.payload.get(1..3) else {
            return Ok(None);
        };
        let size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
        if size == 0 {
            return Ok(None);
        }
        let bytes = self
            .payload
            .get(3..3 + size)
            .ok_or_else(|| anyhow!("restriction data is truncated"))?;
        parse_mapi_restriction(bytes).map(Some)
    }

    fn find_origin(&self) -> Option<u8> {
        let size = u16::from_le_bytes(self.payload.get(1..3)?.try_into().ok()?) as usize;
        self.payload.get(3 + size).copied()
    }

    fn find_backward(&self) -> bool {
        self.payload.first().is_some_and(|flags| flags & 0x01 != 0)
    }

    fn bookmark(&self) -> &[u8] {
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    fn bookmark_row_count(&self) -> Option<i32> {
        let size = u16::from_le_bytes(self.payload.get(..2)?.try_into().ok()?) as usize;
        let bytes = self.payload.get(2 + size..6 + size)?;
        Some(i32::from_le_bytes(bytes.try_into().ok()?))
    }

    fn bookmark_want_row_moved_count(&self) -> bool {
        let Some(size) = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
        else {
            return false;
        };
        self.payload.get(6 + size).is_some_and(|want| *want != 0)
    }

    fn seek_origin(&self) -> Option<u8> {
        self.payload.first().copied()
    }

    fn seek_row_count(&self) -> Option<i32> {
        let bytes = self.payload.get(1..5)?;
        Some(i32::from_le_bytes(bytes.try_into().ok()?))
    }

    fn want_row_moved_count(&self) -> bool {
        self.payload.get(5).is_some_and(|want| *want != 0)
    }

    fn sort_orders(&self) -> Vec<MapiSortOrder> {
        let Some(count_bytes) = self.payload.get(1..3) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(7..)
            .unwrap_or_default()
            .chunks_exact(5)
            .take(count)
            .map(|bytes| MapiSortOrder {
                property_tag: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                order: bytes[4],
            })
            .collect()
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

fn parse_mapi_restriction(bytes: &[u8]) -> Result<MapiRestriction> {
    let mut cursor = Cursor::new(bytes);
    parse_mapi_restriction_from(&mut cursor)
}

fn parse_mapi_restriction_from(cursor: &mut Cursor<'_>) -> Result<MapiRestriction> {
    match cursor.read_u8()? {
        0x00 => {
            let count = cursor.read_u16()? as usize;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(parse_mapi_restriction_from(cursor)?);
            }
            Ok(MapiRestriction::And(children))
        }
        0x01 => {
            let count = cursor.read_u16()? as usize;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(parse_mapi_restriction_from(cursor)?);
            }
            Ok(MapiRestriction::Or(children))
        }
        0x02 => Ok(MapiRestriction::Not(Box::new(parse_mapi_restriction_from(
            cursor,
        )?))),
        0x03 => {
            let _fuzzy_level = cursor.read_u32()?;
            let property_tag = cursor.read_u32()?;
            let value = parse_tagged_property_value(cursor)?
                .into_text()
                .ok_or_else(|| anyhow!("content restriction requires a text value"))?;
            Ok(MapiRestriction::Content {
                property_tag,
                value,
            })
        }
        0x04 => {
            let relop = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let value = parse_tagged_property_value(cursor)?;
            Ok(MapiRestriction::Property {
                relop,
                property_tag,
                value,
            })
        }
        0x06 => {
            let rel_bmr = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let mask = cursor.read_u32()?;
            Ok(MapiRestriction::Bitmask {
                property_tag,
                mask,
                must_be_nonzero: rel_bmr != 0,
            })
        }
        0x07 => {
            let relop = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let size = cursor.read_u32()?;
            Ok(MapiRestriction::Size {
                relop,
                property_tag,
                size,
            })
        }
        0x08 => {
            let property_tag = cursor.read_u32()?;
            Ok(MapiRestriction::Exist { property_tag })
        }
        _ => Err(anyhow!("unsupported MAPI restriction type")),
    }
}

fn parse_tagged_property_value(cursor: &mut Cursor<'_>) -> Result<MapiValue> {
    let property_tag = cursor.read_u32()?;
    match property_tag & 0xFFFF {
        0x0003 => Ok(MapiValue::I32(cursor.read_i32()?)),
        0x000B => Ok(MapiValue::Bool(cursor.read_u8()? != 0)),
        0x0014 => Ok(MapiValue::I64(cursor.read_i64()?)),
        0x001E => Ok(MapiValue::String(cursor.read_ascii_z()?)),
        0x001F => Ok(MapiValue::String(cursor.read_utf16z()?)),
        0x0102 => {
            let len = cursor.read_u16()? as usize;
            Ok(MapiValue::Binary(cursor.read_bytes(len)?.to_vec()))
        }
        _ => Err(anyhow!("unsupported MAPI tagged value type")),
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
        0x04 | 0x05 | 0x21 => {
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
        0x22 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x08 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x2B => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x2C => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u16()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x66 => {
            let input_handle_index = cursor.read_u8()?;
            let want_asynchronous = cursor.read_u8()?;
            let read_flags = cursor.read_u8()?;
            let message_id_count = cursor.read_u16()? as usize;
            let mut payload = vec![want_asynchronous, read_flags];
            payload.extend_from_slice(&(message_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(message_id_count * 8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
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
        0x18 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(cursor.read_u8()?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x19 => {
            let input_handle_index = cursor.read_u8()?;
            let bookmark_size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(bookmark_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(bookmark_size)?);
            payload.extend_from_slice(&cursor.read_i32()?.to_le_bytes());
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x1B => {
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
        0x11 => {
            let response_handle_index = cursor.read_u8()?;
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(response_handle_index),
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
        0x13 => {
            let input_handle_index = cursor.read_u8()?;
            let sort_table_flags = cursor.read_u8()?;
            let sort_order_count = cursor.read_u16()? as usize;
            let category_count = cursor.read_u16()?;
            let expanded_count = cursor.read_u16()?;
            let mut payload = vec![sort_table_flags];
            payload.extend_from_slice(&(sort_order_count as u16).to_le_bytes());
            payload.extend_from_slice(&category_count.to_le_bytes());
            payload.extend_from_slice(&expanded_count.to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(sort_order_count * 5)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x14 => {
            let input_handle_index = cursor.read_u8()?;
            let restrict_flags = cursor.read_u8()?;
            let restriction_size = cursor.read_u16()? as usize;
            let mut payload = vec![restrict_flags];
            payload.extend_from_slice(&(restriction_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(restriction_size)?);
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
        0x4F => {
            let input_handle_index = cursor.read_u8()?;
            let find_row_flags = cursor.read_u8()?;
            let restriction_size = cursor.read_u16()? as usize;
            let mut payload = vec![find_row_flags];
            payload.extend_from_slice(&(restriction_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(restriction_size)?);
            payload.push(cursor.read_u8()?);
            let bookmark_size = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(bookmark_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(bookmark_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x89 => {
            let input_handle_index = cursor.read_u8()?;
            let bookmark_size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(bookmark_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(bookmark_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0xFE => {
            let output_handle_index = cursor.read_u8()?;
            let logon_flags = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.push(logon_flags);
            payload.extend_from_slice(cursor.read_bytes(4)?);
            payload.extend_from_slice(cursor.read_bytes(4)?);
            let essdn_size = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(essdn_size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(essdn_size)?);
            if logon_flags & 0x40 != 0 {
                payload.extend_from_slice(cursor.read_bytes(cursor.remaining())?);
            }
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

    #[test]
    fn logon_time_bytes_encode_valid_utc_calendar_fields() {
        let bytes = logon_time_bytes(SystemTime::UNIX_EPOCH + Duration::from_secs(1_778_046_495));

        assert_eq!(bytes, [15, 48, 5, 3, 6, 5, 0xEA, 0x07]);
    }

    #[test]
    fn gwart_time_marker_uses_real_timestamp_and_stays_nonzero() {
        assert_eq!(
            gwart_time_marker(SystemTime::UNIX_EPOCH + Duration::from_secs(1_778_046_495)),
            1_778_046_495
        );
        assert_eq!(gwart_time_marker(SystemTime::UNIX_EPOCH), 1);
    }
}
