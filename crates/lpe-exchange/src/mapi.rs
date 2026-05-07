use anyhow::{anyhow, Result};
use axum::{
    http::{
        header::{CONTENT_TYPE, SET_COOKIE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use lpe_magika::{
    Detector, ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{
    serialize_calendar_participants_metadata, AccessibleContact, AccessibleEvent,
    AttachmentUploadInput, AuditEntryInput, CalendarParticipantsMetadata, CollaborationRights,
    JmapEmail, JmapEmailAddress, JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput,
    SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput, UpsertClientContactInput,
    UpsertClientEventInput,
};
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
    mapi_mailstore,
    mapi_store::{
        MapiAttachment, MapiCollaborationFolder, MapiCollaborationFolderKind,
        MapiMailStoreSnapshot, MapiStore,
    },
    store::{ExchangeAddressBookEntry, ExchangeAddressBookEntryKind, ExchangeStore},
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
    message_statuses: HashMap<(u64, u64), u32>,
    named_properties: HashMap<MapiNamedProperty, u16>,
    named_property_ids: HashMap<u16, MapiNamedProperty>,
    next_named_property_id: u16,
    next_local_replica_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct MapiNamedProperty {
    guid: [u8; 16],
    kind: MapiNamedPropertyKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum MapiNamedPropertyKind {
    Lid(u32),
    Name(String),
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
struct PendingRecipient {
    row_id: u32,
    recipient_type: u8,
    address: String,
    display_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingRecipientChange {
    Upsert(PendingRecipient),
    Delete(u32),
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
    Contact {
        folder_id: u64,
        contact_id: u64,
    },
    Event {
        folder_id: u64,
        event_id: u64,
    },
    PendingMessage {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
        recipients: Vec<PendingRecipient>,
    },
    PendingContact {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
    },
    PendingEvent {
        folder_id: u64,
        properties: HashMap<u32, MapiValue>,
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
    PendingAttachment {
        folder_id: u64,
        message_id: u64,
        attach_num: u32,
        properties: HashMap<u32, MapiValue>,
        data: Vec<u8>,
    },
    SavedAttachment {
        folder_id: u64,
        message_id: u64,
        attach_num: u32,
        file_reference: String,
        file_name: String,
        media_type: String,
        size_octets: u64,
    },
    AttachmentStream {
        data: Vec<u8>,
        position: usize,
        writable_attachment_handle: Option<u32>,
    },
    SynchronizationSource {
        folder_id: u64,
        sync_type: u8,
        state: Vec<u8>,
        state_upload_buffer: Vec<u8>,
        transfer_buffer: Vec<u8>,
        transfer_position: usize,
    },
    SynchronizationCollector {
        folder_id: u64,
        state: Vec<u8>,
        state_upload_buffer: Vec<u8>,
    },
}

static MAPI_SESSIONS: OnceLock<Mutex<HashMap<String, MapiSession>>> = OnceLock::new();

fn sessions() -> &'static Mutex<HashMap<String, MapiSession>> {
    MAPI_SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) async fn handle_mapi<S, V>(
    store: &S,
    validator: &Validator<V>,
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
    _body: &[u8],
) -> Result<Response>
where
    S: ExchangeStore,
    V: Detector,
{
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
            execute_response(
                store,
                validator,
                endpoint,
                &principal,
                headers,
                _body,
                &request_id,
            )
            .await
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
        (MapiEndpoint::Nspi, MapiRequestType::DnToMid) => {
            nspi_dn_to_mid_response(store, &principal, _body, &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetMatches) => {
            nspi_matches_response(store, &principal, _body, &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetPropList) => {
            nspi_property_tags_response("GetPropList", &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetProps) => {
            nspi_props_response(store, &principal, _body, "GetProps", &request_id).await
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
            nspi_rowset_response(store, &principal, _body, "QueryRows", &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::ResolveNames) => {
            resolve_names_response(store, &principal, _body, &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::ResortRestriction) => {
            nspi_minimal_ids_response("ResortRestriction", &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::SeekEntries) => {
            nspi_rowset_response(store, &principal, _body, "SeekEntries", &request_id).await
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

async fn execute_response<S, V>(
    store: &S,
    validator: &Validator<V>,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    body: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
    V: Detector,
{
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
        validator,
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

async fn resolve_names_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let columns = resolve_names_columns(request);
    let requested_names = resolve_names_requested_values(request);
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
        Ok(entries) => entries,
        Err(error) => {
            return mapi_diagnostic_response(
                "ResolveNames",
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    let matched = requested_names
        .first()
        .and_then(|name| nspi_match_entry(&entries, name))
        .or_else(|| {
            requested_names
                .is_empty()
                .then(|| {
                    entries
                        .iter()
                        .find(|entry| nspi_entry_is_principal(entry, principal))
                })
                .flatten()
        });

    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    write_u32(&mut body, 1);
    write_u32(
        &mut body,
        if matched.is_some() {
            NSPI_MID_RESOLVED
        } else {
            0
        },
    );
    if let Some(entry) = matched {
        body.push(1);
        write_large_property_tag_array(&mut body, &columns);
        write_u32(&mut body, 1);
        body.extend_from_slice(&nspi_resolved_entry_row(entry, &columns));
    } else {
        body.push(0);
    }
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

fn resolve_names_requested_values(request: &[u8]) -> Vec<String> {
    parse_resolve_names_values(request).unwrap_or_else(|| scan_address_book_lookup_values(request))
}

fn parse_resolve_names_values(request: &[u8]) -> Option<Vec<String>> {
    let mut cursor = Cursor::new(request);
    let _reserved = cursor.read_u32().ok()?;
    if cursor.read_u8().ok()? != 0 {
        cursor.read_bytes(36).ok()?;
    }
    if cursor.read_u8().ok()? != 0 {
        let count = cursor.read_u32().ok()? as usize;
        if count > 128 {
            return None;
        }
        cursor.read_bytes(count.checked_mul(4)?).ok()?;
    }
    if cursor.read_u8().ok()? == 0 {
        return Some(Vec::new());
    }
    let count = cursor.read_u32().ok()? as usize;
    if count > 128 {
        return None;
    }
    let mut values = Vec::new();
    for _ in 0..count {
        let size = cursor.read_u16().ok()? as usize;
        let bytes = cursor.read_bytes(size).ok()?;
        if let Some(value) = decode_utf16le_string(bytes) {
            let value = normalize_nspi_lookup_value(&value);
            if !value.is_empty() {
                values.push(value);
            }
        }
    }
    Some(values)
}

fn nspi_u32_result_response(request_type: &str, request_id: &str, value: u32) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, value);
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

async fn nspi_dn_to_mid_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
        Ok(entries) => entries,
        Err(error) => {
            return mapi_diagnostic_response(
                "DNToMId",
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    let values = scan_address_book_lookup_values(request);
    let entry = values
        .first()
        .and_then(|value| nspi_match_entry(&entries, value))
        .or_else(|| {
            values
                .is_empty()
                .then(|| {
                    entries
                        .iter()
                        .find(|entry| nspi_entry_is_principal(entry, principal))
                })
                .flatten()
        });
    nspi_u32_result_response("DNToMId", request_id, entry.map(nspi_entry_id).unwrap_or(0))
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

async fn nspi_props_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_type: &str,
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
        Ok(entries) => entries,
        Err(error) => {
            return mapi_diagnostic_response(
                request_type,
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    let entry = nspi_requested_entry(request, &entries).or_else(|| {
        entries
            .iter()
            .find(|entry| nspi_entry_is_principal(entry, principal))
    });
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    if let Some(entry) = entry {
        body.push(1);
        body.extend_from_slice(&nspi_entry_property_value_list(
            entry,
            NSPI_BOOTSTRAP_PROPERTY_TAGS,
        ));
    } else {
        body.push(0);
    }
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

async fn nspi_rowset_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_type: &str,
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
        Ok(entries) => nspi_filter_entries_for_request(entries, request),
        Err(error) => {
            return mapi_diagnostic_response(
                request_type,
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push((!entries.is_empty()) as u8);
    if !entries.is_empty() {
        write_large_property_tag_array(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS);
        write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
        for entry in &entries {
            body.extend_from_slice(&nspi_resolved_entry_row(
                entry,
                NSPI_BOOTSTRAP_PROPERTY_TAGS,
            ));
        }
    }
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

async fn nspi_matches_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
        Ok(entries) => nspi_filter_entries_for_request(entries, request),
        Err(error) => {
            return mapi_diagnostic_response(
                "GetMatches",
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push((!entries.is_empty()) as u8);
    write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
    for entry in &entries {
        write_u32(&mut body, nspi_entry_id(entry));
    }
    body.push((!entries.is_empty()) as u8);
    if !entries.is_empty() {
        write_large_property_tag_array(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS);
        write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
        for entry in &entries {
            body.extend_from_slice(&nspi_resolved_entry_row(
                entry,
                NSPI_BOOTSTRAP_PROPERTY_TAGS,
            ));
        }
    }
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
    let entry = principal_address_book_entry(principal);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    body.extend_from_slice(&nspi_entry_property_value_list(
        &entry,
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

fn nspi_resolved_entry_row(entry: &ExchangeAddressBookEntry, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    row.push(0);
    for property_tag in columns {
        write_address_book_property_value(
            &mut row,
            *property_tag,
            &nspi_entry_value(entry, *property_tag),
        );
    }
    row
}

fn nspi_entry_property_value_list(entry: &ExchangeAddressBookEntry, tags: &[u32]) -> Vec<u8> {
    let mut values = Vec::new();
    write_u32(&mut values, tags.len() as u32);
    for property_tag in tags {
        write_address_book_tagged_property_value(
            &mut values,
            *property_tag,
            &nspi_entry_value(entry, *property_tag),
        );
    }
    values
}

enum NspiValue<'a> {
    String(&'a str),
    OwnedString(String),
    U32(u32),
}

fn nspi_entry_value(entry: &ExchangeAddressBookEntry, property_tag: u32) -> NspiValue<'_> {
    match property_tag {
        0x3001_001F => NspiValue::String(&entry.display_name),
        0x39FE_001F => NspiValue::String(&entry.email),
        0x3003_001F => NspiValue::String(&entry.email),
        0x3A00_001F => NspiValue::String(&entry.display_name),
        0x0FFE_0003 => NspiValue::U32(MAPI_MAILUSER_OBJECT_TYPE),
        0x3000_0003 => NspiValue::U32(nspi_entry_id(entry)),
        0x3004_001F => NspiValue::String(&entry.email),
        0x3002_001F => NspiValue::String("SMTP"),
        0x3005_001F => NspiValue::OwnedString(nspi_entry_legacy_dn(entry)),
        _ => match property_tag & 0xFFFF {
            0x001F => NspiValue::String(""),
            0x0003 => NspiValue::U32(0),
            _ => NspiValue::U32(0),
        },
    }
}

fn nspi_entry_id(entry: &ExchangeAddressBookEntry) -> u32 {
    let bytes = entry.id.as_bytes();
    let value = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => value | 0x8000_0000,
        ExchangeAddressBookEntryKind::Contact => value | 0x4000_0000,
    }
    .max(2)
}

fn principal_minimal_entry_id(principal: &AccountPrincipal) -> u32 {
    nspi_entry_id(&principal_address_book_entry(principal))
}

fn principal_address_book_entry(principal: &AccountPrincipal) -> ExchangeAddressBookEntry {
    ExchangeAddressBookEntry {
        id: principal.account_id,
        display_name: principal.display_name.clone(),
        email: principal.email.clone(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
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

fn nspi_entry_legacy_dn(entry: &ExchangeAddressBookEntry) -> String {
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
    format!("/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={prefix}-{legacy_user}")
}

fn nspi_entry_is_principal(entry: &ExchangeAddressBookEntry, principal: &AccountPrincipal) -> bool {
    entry.entry_kind == ExchangeAddressBookEntryKind::Account && entry.id == principal.account_id
}

fn nspi_requested_entry<'a>(
    request: &[u8],
    entries: &'a [ExchangeAddressBookEntry],
) -> Option<&'a ExchangeAddressBookEntry> {
    let ids = nspi_requested_entry_ids(request);
    ids.iter()
        .find_map(|id| entries.iter().find(|entry| nspi_entry_id(entry) == *id))
        .or_else(|| {
            scan_address_book_lookup_values(request)
                .iter()
                .find_map(|value| nspi_match_entry(entries, value))
        })
}

fn nspi_filter_entries_for_request(
    entries: Vec<ExchangeAddressBookEntry>,
    request: &[u8],
) -> Vec<ExchangeAddressBookEntry> {
    let values = scan_address_book_lookup_values(request);
    if values.is_empty() {
        return entries;
    }
    entries
        .into_iter()
        .filter(|entry| values.iter().any(|value| nspi_entry_matches(entry, value)))
        .collect()
}

fn nspi_match_entry<'a>(
    entries: &'a [ExchangeAddressBookEntry],
    value: &str,
) -> Option<&'a ExchangeAddressBookEntry> {
    entries
        .iter()
        .find(|entry| nspi_entry_matches(entry, value) && nspi_entry_exact_match(entry, value))
        .or_else(|| {
            entries
                .iter()
                .find(|entry| nspi_entry_matches(entry, value))
        })
}

fn nspi_entry_exact_match(entry: &ExchangeAddressBookEntry, value: &str) -> bool {
    let value = normalize_nspi_lookup_value(value);
    let legacy_dn = nspi_entry_legacy_dn(entry).to_ascii_lowercase();
    value == entry.email.to_ascii_lowercase()
        || value == entry.display_name.to_ascii_lowercase()
        || value == legacy_dn
        || value == format!("smtp:{}", entry.email.to_ascii_lowercase())
        || value == format!("=smtp:{}", entry.email.to_ascii_lowercase())
}

fn nspi_entry_matches(entry: &ExchangeAddressBookEntry, value: &str) -> bool {
    let value = normalize_nspi_lookup_value(value);
    if value.is_empty() {
        return false;
    }
    nspi_entry_exact_match(entry, &value)
        || entry
            .display_name
            .to_ascii_lowercase()
            .contains(value.as_str())
        || entry.email.to_ascii_lowercase().contains(value.as_str())
        || nspi_entry_legacy_dn(entry)
            .to_ascii_lowercase()
            .contains(value.as_str())
}

fn nspi_requested_entry_ids(request: &[u8]) -> Vec<u32> {
    let mut ids = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= request.len() {
        let value = u32::from_le_bytes([
            request[offset],
            request[offset + 1],
            request[offset + 2],
            request[offset + 3],
        ]);
        if value >= 2 && !ids.contains(&value) {
            ids.push(value);
        }
        offset += 4;
    }
    ids
}

fn scan_address_book_lookup_values(request: &[u8]) -> Vec<String> {
    let mut values = Vec::new();
    values.extend(scan_ascii_lookup_values(request));
    values.extend(scan_utf16_lookup_values(request));
    values.sort();
    values.dedup();
    values
}

fn scan_ascii_lookup_values(request: &[u8]) -> Vec<String> {
    request
        .split(|byte| *byte == 0)
        .filter_map(|bytes| {
            if bytes.len() < 3 {
                return None;
            }
            let value = String::from_utf8_lossy(bytes);
            let value = normalize_nspi_lookup_value(&value);
            (!value.is_empty() && (value.contains('@') || value.contains("/cn="))).then_some(value)
        })
        .collect()
}

fn scan_utf16_lookup_values(request: &[u8]) -> Vec<String> {
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
                let value = normalize_nspi_lookup_value(&value);
                if !value.is_empty() && (value.contains('@') || value.contains("/cn=")) {
                    values.push(value);
                }
            }
        }
        start += 1;
    }
    values
}

fn decode_utf16le_string(bytes: &[u8]) -> Option<String> {
    if bytes.len() % 2 != 0 {
        return None;
    }
    let units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .take_while(|unit| *unit != 0)
        .collect::<Vec<_>>();
    String::from_utf16(&units).ok()
}

fn normalize_nspi_lookup_value(value: &str) -> String {
    let mut value = value.trim().trim_matches('\0').to_ascii_lowercase();
    if let Some(rest) = value.strip_prefix("=smtp:") {
        value = rest.to_string();
    } else if let Some(rest) = value.strip_prefix("smtp:") {
        value = rest.to_string();
    }
    value
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
        message_statuses: HashMap::new(),
        named_properties: HashMap::new(),
        named_property_ids: HashMap::new(),
        next_named_property_id: FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
    };
    let mut guard = sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    prune_expired_sessions_locked(&mut guard, now);
    guard.insert(session_id.clone(), session);
    session_id
}

pub(crate) fn create_rpc_emsmdb_context(principal: &AccountPrincipal) -> [u8; 20] {
    let session_id = create_session(MapiEndpoint::Emsmdb, principal);
    let session_uuid = Uuid::parse_str(&session_id).unwrap_or_else(|_| Uuid::new_v4());
    let mut context = [0u8; 20];
    context[4..20].copy_from_slice(session_uuid.as_bytes());
    context
}

pub(crate) async fn execute_rpc_emsmdb_rops<S, V>(
    store: &S,
    validator: &Validator<V>,
    principal: &AccountPrincipal,
    context_handle: &[u8],
    rop_buffer: &[u8],
) -> Result<Vec<u8>>
where
    S: ExchangeStore,
    V: Detector,
{
    let session_id = rpc_context_session_id(context_handle)
        .ok_or_else(|| anyhow!("invalid RPC/HTTP EMSMDB context handle"))?;
    let Some(session) = get_session(&session_id) else {
        return Err(anyhow!("RPC/HTTP EMSMDB session context not found"));
    };
    if !session_matches(&session, MapiEndpoint::Emsmdb, principal) {
        return Err(anyhow!("RPC/HTTP EMSMDB authentication context changed"));
    }

    let snapshot = store
        .load_mapi_mail_store(principal.account_id, 500)
        .await?;
    let mailboxes = snapshot.mailboxes();
    let emails = snapshot.emails();
    let Some(mut session) = remove_session(&session_id) else {
        return Err(anyhow!("RPC/HTTP EMSMDB session context not found"));
    };
    if !session_matches(&session, MapiEndpoint::Emsmdb, principal) {
        return Err(anyhow!("RPC/HTTP EMSMDB authentication context changed"));
    }
    let rop_buffer = execute_rops(
        store,
        principal,
        &mut session,
        &mailboxes,
        &emails,
        &snapshot,
        validator,
        rop_buffer,
    )
    .await;
    store_session(session_id, session);
    Ok(rop_buffer)
}

fn rpc_context_session_id(context_handle: &[u8]) -> Option<String> {
    if context_handle.len() < 20 {
        return None;
    }
    let uuid = Uuid::from_slice(&context_handle[4..20]).ok()?;
    Some(uuid.to_string())
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

async fn execute_rops<S, V>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    validator: &Validator<V>,
    rop_buffer: &[u8],
) -> Vec<u8>
where
    S: ExchangeStore,
    V: Detector,
{
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
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &email.subject,
                        message_recipients(email).len(),
                    ));
                    output_handles.push(handle);
                } else if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Contact {
                            folder_id,
                            contact_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &contact.contact.name,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Event {
                            folder_id,
                            event_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &event.event.title,
                        0,
                    ));
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
                    hierarchy_row_count(folder_id, mailboxes, snapshot),
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
                    folder_message_count(folder_id, mailboxes, emails, snapshot),
                ));
                output_handles.push(handle);
            }
            0x06 => {
                let folder_id = request.folder_id().unwrap_or_else(|| {
                    input_object(session, &handle_slots, &request)
                        .and_then(MapiObject::folder_id)
                        .unwrap_or(INBOX_FOLDER_ID)
                });
                if snapshot.collaboration_folder_for_id(folder_id).is_none()
                    && folder_row_for_id(folder_id, mailboxes).is_none()
                    && !matches!(
                        folder_id,
                        INBOX_FOLDER_ID
                            | DRAFTS_FOLDER_ID
                            | SENT_FOLDER_ID
                            | TRASH_FOLDER_ID
                            | OUTBOX_FOLDER_ID
                    )
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x06,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let pending_object = match snapshot
                    .collaboration_folder_for_id(folder_id)
                    .map(|folder| folder.kind)
                {
                    Some(MapiCollaborationFolderKind::Contacts) => MapiObject::PendingContact {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    Some(MapiCollaborationFolderKind::Calendar) => MapiObject::PendingEvent {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    _ => MapiObject::PendingMessage {
                        folder_id,
                        properties: HashMap::new(),
                        recipients: Vec::new(),
                    },
                };
                let handle =
                    session.allocate_output_handle(request.output_handle_index, pending_object);
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_message_response(&request));
                output_handles.push(handle);
            }
            0x07 => responses.extend_from_slice(&rop_get_properties_specific_response(
                &request,
                input_object(session, &handle_slots, &request),
                principal,
                mailboxes,
                emails,
                snapshot,
            )),
            0x08 => responses.extend_from_slice(&rop_get_properties_all_response(
                &request,
                input_object(session, &handle_slots, &request),
                principal,
                mailboxes,
                emails,
                snapshot,
            )),
            0x09 => responses.extend_from_slice(&rop_get_properties_list_response(
                &request,
                input_object(session, &handle_slots, &request),
            )),
            0x0A | 0x79 => {
                let set_result = match request.property_values() {
                    Ok(values) => match input_object(session, &handle_slots, &request).cloned() {
                        Some(MapiObject::Message {
                            folder_id,
                            message_id,
                        }) => {
                            apply_canonical_message_property_values(
                                store, principal, folder_id, message_id, values, mailboxes, emails,
                            )
                            .await
                        }
                        Some(MapiObject::Contact {
                            folder_id,
                            contact_id,
                        }) => {
                            apply_canonical_contact_property_values(
                                store, principal, folder_id, contact_id, values, snapshot,
                            )
                            .await
                        }
                        Some(MapiObject::Event {
                            folder_id,
                            event_id,
                        }) => {
                            apply_canonical_event_property_values(
                                store, principal, folder_id, event_id, values, snapshot,
                            )
                            .await
                        }
                        _ => apply_mapi_property_values(
                            input_object_mut(session, &handle_slots, &request),
                            values,
                        ),
                    },
                    Err(error) => Err(error),
                };
                match set_result {
                    Ok(()) => responses.extend_from_slice(&rop_set_properties_response(&request)),
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            0x0B | 0x7A => {
                let deleted = delete_mapi_properties(
                    input_object_mut(session, &handle_slots, &request),
                    &request.property_tags(),
                );
                match deleted {
                    Ok(()) => {
                        responses.extend_from_slice(&rop_delete_properties_response(&request))
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            0x0C => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match session.handles.get(&handle).cloned() {
                    Some(MapiObject::PendingContact {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = contact_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_contact_for_mapping(
                                principal.account_id,
                                &folder.collection.id,
                            ),
                            &properties,
                        );
                        match store
                            .create_accessible_contact(
                                principal.account_id,
                                Some(&folder.collection.id),
                                input,
                            )
                            .await
                        {
                            Ok(contact) => {
                                let contact_id = mapi_item_id(&contact.id);
                                session.handles.insert(
                                    handle,
                                    MapiObject::Contact {
                                        folder_id,
                                        contact_id,
                                    },
                                );
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, contact_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingEvent {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = match event_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_event_for_mapping(principal.account_id, &folder.collection.id),
                            &properties,
                        ) {
                            Ok(input) => input,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        match store
                            .create_accessible_event(
                                principal.account_id,
                                Some(&folder.collection.id),
                                input,
                            )
                            .await
                        {
                            Ok(event) => {
                                let event_id = mapi_item_id(&event.id);
                                session.handles.insert(
                                    handle,
                                    MapiObject::Event {
                                        folder_id,
                                        event_id,
                                    },
                                );
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, event_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::Contact { contact_id, .. })
                    | Some(MapiObject::Event {
                        event_id: contact_id,
                        ..
                    }) => {
                        responses.extend_from_slice(&rop_save_changes_message_response(
                            &request, contact_id,
                        ));
                        continue;
                    }
                    _ => {}
                }
                let Some(MapiObject::PendingMessage {
                    folder_id,
                    properties,
                    recipients,
                }) = session.handles.get(&handle).cloned()
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let input =
                    jmap_import_from_pending_message(principal, mailbox, &properties, &recipients);
                match store
                    .import_jmap_email(
                        input,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-save-message".to_string(),
                            subject: format!("folder:{}", mailbox.id),
                        },
                    )
                    .await
                {
                    Ok(email) => {
                        let message_id = mapi_message_id(&email);
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id,
                                message_id,
                            },
                        );
                        responses.extend_from_slice(&rop_save_changes_message_response(
                            &request, message_id,
                        ));
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x0D => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::PendingMessage { recipients, .. }) => {
                    recipients.clear();
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x0D,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x0E => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::PendingMessage { recipients, .. }) => {
                    match request.modify_recipients() {
                        Ok(changes) => {
                            apply_pending_recipient_changes(recipients, changes);
                            responses.extend_from_slice(&rop_simple_success_response(&request));
                        }
                        Err(_) => responses.extend_from_slice(&rop_error_response(
                            0x0E,
                            request.response_handle_index(),
                            0x8004_0102,
                        )),
                    }
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x0E,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x0F => responses.extend_from_slice(&rop_read_recipients_response(
                &request,
                input_object(session, &handle_slots, &request),
                mailboxes,
                emails,
            )),
            0x10 => responses.extend_from_slice(&rop_reload_cached_information_response(
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
            0x1C => {
                let parent_folder_id = match input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x1C,
                            request.output_handle_index.unwrap_or(0),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                if !is_root_hierarchy_folder(parent_folder_id)
                    && folder_row_for_id(parent_folder_id, mailboxes).is_none()
                    && role_for_folder_id(parent_folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let display_name = request.create_folder_display_name();
                let display_name = display_name.trim();
                if display_name.is_empty() || request.create_folder_type() == 0 {
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    ));
                    continue;
                }

                if request.create_folder_open_existing() {
                    if let Some(existing) = mailboxes
                        .iter()
                        .find(|mailbox| mailbox.name.eq_ignore_ascii_case(display_name))
                    {
                        let folder_id = mapi_folder_id(existing);
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder { folder_id },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, true,
                        ));
                        output_handles.push(handle);
                        continue;
                    }
                }

                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.to_string(),
                            sort_order: None,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-create-folder".to_string(),
                            subject: display_name.to_string(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => {
                        let folder_id = mapi_folder_id(&mailbox);
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder { folder_id },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, false,
                        ));
                        output_handles.push(handle);
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    )),
                }
            }
            0x1D => {
                let Some(_parent_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(folder_id) = request.delete_folder_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if mailbox.role != "custom" {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8007_0005,
                    ));
                    continue;
                }

                let partial_completion = store
                    .destroy_jmap_mailbox(
                        principal.account_id,
                        mailbox.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-folder".to_string(),
                            subject: format!("folder:{}", mailbox.id),
                        },
                    )
                    .await
                    .is_err();
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x1D,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            0x1E | 0x91 => {
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let mut partial_completion = false;
                for message_id in request.message_ids() {
                    if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_contact(principal.account_id, contact.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_event(principal.account_id, event.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if request.rop_id == 0x91 || email.mailbox_role == "trash" {
                        store
                            .delete_jmap_email(
                                principal.account_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else if let Some(trash_mailbox) =
                        mailboxes.iter().find(|mailbox| mailbox.role == "trash")
                    {
                        store
                            .move_jmap_email(
                                principal.account_id,
                                email.id,
                                trash_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-message-to-trash".to_string(),
                                    subject: format!("message:{}->{}", email.id, trash_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .delete_jmap_email(
                                principal.account_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message-without-trash".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    request.rop_id,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            0x1F | 0x20 => {
                let folder_id = match input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x20,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let message_id = request.status_message_id().unwrap_or(0);
                if message_for_id(folder_id, message_id, mailboxes, emails).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x20,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let key = (folder_id, message_id);
                let old_status = session.message_statuses.get(&key).copied().unwrap_or(0);
                if request.rop_id == 0x20 {
                    let mask = request.message_status_mask();
                    let new_status = (old_status & !mask) | (request.message_status_flags() & mask);
                    if new_status == 0 {
                        session.message_statuses.remove(&key);
                    } else {
                        session.message_statuses.insert(key, new_status);
                    }
                }
                responses.extend_from_slice(&rop_message_status_response(&request, old_status));
            }
            0x4F => responses.extend_from_slice(&rop_find_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            0x52 => responses.extend_from_slice(&rop_get_valid_attachments_response(
                &request,
                input_object(session, &handle_slots, &request),
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
            0x23 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x0000_04B9,
                    ));
                    continue;
                };
                if message_for_id(*folder_id, *message_id, mailboxes, emails).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let attach_num =
                    next_pending_attachment_num(session, *folder_id, *message_id, snapshot);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::PendingAttachment {
                        folder_id: *folder_id,
                        message_id: *message_id,
                        attach_num,
                        properties: HashMap::new(),
                        data: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_attachment_response(&request, attach_num));
                output_handles.push(handle);
            }
            0x24 => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                let Some(attachment) =
                    snapshot.attachment_for_message(*folder_id, *message_id, attach_num)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match store
                    .delete_message_attachment(
                        principal.account_id,
                        &attachment.file_reference,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-attachment".to_string(),
                            subject: attachment.file_reference.clone(),
                        },
                    )
                    .await
                {
                    Ok(Some(_)) => {
                        responses.extend_from_slice(&rop_simple_success_response(&request))
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x25 => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(MapiObject::PendingAttachment {
                    folder_id,
                    message_id,
                    attach_num,
                    properties,
                    data,
                }) = session.handles.get(&handle).cloned()
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let attachment = pending_attachment_upload(attach_num, &properties, data);
                let validation = validator.validate_bytes(
                    ValidationRequest {
                        ingress_context: IngressContext::ExchangeAttachment,
                        declared_mime: Some(attachment.media_type.clone()),
                        filename: Some(attachment.file_name.clone()),
                        expected_kind: mapi_expected_attachment_kind(
                            &attachment.media_type,
                            &attachment.file_name,
                        ),
                    },
                    &attachment.blob_bytes,
                );
                let Ok(outcome) = validation else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if outcome.policy_decision != PolicyDecision::Accept {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let mut attachment = attachment;
                if attachment.media_type == "application/octet-stream"
                    && !outcome.detected_mime.trim().is_empty()
                {
                    attachment.media_type = outcome.detected_mime;
                }
                match store
                    .add_message_attachment(
                        principal.account_id,
                        email.id,
                        attachment,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-save-attachment".to_string(),
                            subject: format!("message:{}", email.id),
                        },
                    )
                    .await
                {
                    Ok(Some((_email, stored))) => {
                        session.handles.insert(
                            handle,
                            MapiObject::SavedAttachment {
                                folder_id,
                                message_id,
                                attach_num,
                                file_reference: stored.file_reference,
                                file_name: stored.file_name,
                                media_type: stored.media_type,
                                size_octets: stored.size_octets,
                            },
                        );
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x2B => {
                if request.stream_property_tag() != Some(PID_TAG_ATTACH_DATA_BINARY) {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    ));
                    continue;
                }
                let Some(input_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some((stream_data, writable_attachment_handle)) =
                    attachment_stream_data(store, principal, session, input_handle, snapshot).await
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let stream_size = stream_data.len();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentStream {
                        data: stream_data,
                        position: 0,
                        writable_attachment_handle,
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
            0x2D | 0x90 => {
                let Some(stream_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match write_attachment_stream(session, stream_handle, request.stream_write_data()) {
                    Some(written) => {
                        responses.extend_from_slice(&rop_write_stream_response(&request, written))
                    }
                    None => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x5D => match input_object(session, &handle_slots, &request) {
                Some(MapiObject::AttachmentStream { .. }) => {
                    responses.extend_from_slice(&rop_simple_success_response(&request))
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x5D,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            },
            0x32 => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x32,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(object) = session.handles.get(&handle).cloned() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x32,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let input = match object {
                    MapiObject::PendingMessage {
                        properties,
                        recipients,
                        ..
                    } => mapi_submit_from_pending_message(principal, &properties, &recipients),
                    MapiObject::Message {
                        folder_id,
                        message_id,
                    } => {
                        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                        else {
                            responses.extend_from_slice(&rop_error_response(
                                0x32,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        if email.mailbox_role != "drafts" {
                            responses.extend_from_slice(&rop_error_response(
                                0x32,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            continue;
                        }
                        mapi_submit_from_email(principal, email)
                    }
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x32,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                match store
                    .submit_message(
                        input,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-submit-message".to_string(),
                            subject: format!("handle:{handle}"),
                        },
                    )
                    .await
                {
                    Ok(submitted) => {
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id: submitted_mapi_folder_id(&submitted, mailboxes),
                                message_id: mapi_store_id(uuid_global_counter(
                                    &submitted.message_id,
                                )),
                            },
                        );
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x32,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x33 => {
                let source_folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let target_folder_id = match request
                    .move_copy_target_handle(&handle_slots)
                    .and_then(|handle| {
                        session
                            .handles
                            .get(&handle)
                            .and_then(|object| object.folder_id())
                    }) {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                };
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x33,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                for message_id in request.move_copy_message_ids() {
                    let Some(email) =
                        message_for_id(source_folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if request.move_copy_want_copy() {
                        store
                            .copy_jmap_email(
                                principal.account_id,
                                email.id,
                                target_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-copy-message".to_string(),
                                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .move_jmap_email(
                                principal.account_id,
                                email.id,
                                target_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-message".to_string(),
                                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x33,
                    request.response_handle_index(),
                    partial_completion,
                ));
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
            0x70 => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x70,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let sync_emails = emails_for_folder(folder_id, mailboxes, emails)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let state = mapi_mailstore::sync_state_token(mailboxes, &sync_emails);
                let transfer_buffer =
                    mapi_mailstore::sync_manifest_buffer(folder_id, mailboxes, &sync_emails);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        sync_type: request.sync_type(),
                        state,
                        state_upload_buffer: Vec::new(),
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_synchronization_configure_response(&request));
                output_handles.push(handle);
            }
            0x4E => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource {
                    sync_type,
                    transfer_buffer,
                    transfer_position,
                    ..
                }) => {
                    let _ = *sync_type;
                    responses.extend_from_slice(&rop_fast_transfer_source_get_buffer_response(
                        &request,
                        transfer_buffer,
                        transfer_position,
                    ));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x4E,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x75 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource {
                    state_upload_buffer,
                    ..
                })
                | Some(MapiObject::SynchronizationCollector {
                    state_upload_buffer,
                    ..
                }) => {
                    state_upload_buffer.clear();
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x75,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x76 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource {
                    state_upload_buffer,
                    ..
                })
                | Some(MapiObject::SynchronizationCollector {
                    state_upload_buffer,
                    ..
                }) => {
                    state_upload_buffer.extend_from_slice(request.stream_data());
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x76,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x77 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource {
                    state,
                    state_upload_buffer,
                    ..
                })
                | Some(MapiObject::SynchronizationCollector {
                    state,
                    state_upload_buffer,
                    ..
                }) => {
                    if !state_upload_buffer.is_empty() {
                        *state = std::mem::take(state_upload_buffer);
                    }
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x77,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x7E => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x7E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationCollector {
                        folder_id,
                        state: Vec::new(),
                        state_upload_buffer: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_simple_success_response(&request));
                output_handles.push(handle);
            }
            0x82 => {
                let Some((folder_id, state)) =
                    synchronization_context_state(input_object(session, &handle_slots, &request))
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x82,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let transfer_buffer = if state.is_empty() {
                    mapi_mailstore::sync_state_token(mailboxes, emails)
                } else {
                    state
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        sync_type: 0,
                        state: transfer_buffer.clone(),
                        state_upload_buffer: Vec::new(),
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses
                    .extend_from_slice(&rop_synchronization_get_transfer_state_response(&request));
                output_handles.push(handle);
            }
            0x72 => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x72,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let property_values = match request.import_property_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let message_id = request.import_message_id().unwrap_or(0);
                if message_id != 0
                    && message_for_id(folder_id, message_id, mailboxes, emails).is_some()
                {
                    if apply_canonical_message_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        mailboxes,
                        emails,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request, message_id),
                    );
                    output_handles.push(handle);
                } else {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::PendingMessage {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                            recipients: Vec::new(),
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request, 0),
                    );
                    output_handles.push(handle);
                }
            }
            0x73 => {
                let Some(_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let (hierarchy_values, property_values) = match request.import_hierarchy_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let display_name = hierarchy_display_name(&hierarchy_values, &property_values);
                let Some(display_name) = display_name else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if system_folder_display_name(&display_name) {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                if let Some(existing) =
                    imported_hierarchy_existing_mailbox(&hierarchy_values, &display_name, mailboxes)
                {
                    if existing.role == "custom"
                        && existing.name.eq_ignore_ascii_case(&display_name)
                    {
                        responses.extend_from_slice(
                            &rop_synchronization_import_hierarchy_change_response(
                                &request,
                                mapi_folder_id(existing),
                            ),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                    }
                    continue;
                }

                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.clone(),
                            sort_order: None,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-sync-import-hierarchy-change".to_string(),
                            subject: display_name.clone(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => responses.extend_from_slice(
                        &rop_synchronization_import_hierarchy_change_response(
                            &request,
                            mapi_folder_id(&mailbox),
                        ),
                    ),
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            0x74 => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x74,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                for message_id in request.import_delete_message_ids() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if store
                        .delete_jmap_email(
                            principal.account_id,
                            email.id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-sync-import-delete".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x74,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            0x78 => {
                let Some((message_id, target_folder_id)) = request.import_move() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let source_folder_id = input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                    .unwrap_or(INBOX_FOLDER_ID);
                let Some(email) = message_for_id(source_folder_id, message_id, mailboxes, emails)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match store
                    .move_jmap_email(
                        principal.account_id,
                        email.id,
                        target_mailbox.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-sync-import-move".to_string(),
                            subject: format!("message:{}->{}", email.id, target_mailbox.id),
                        },
                    )
                    .await
                {
                    Ok(moved) => responses.extend_from_slice(
                        &rop_synchronization_import_message_move_response(
                            &request,
                            mapi_message_id(&moved),
                        ),
                    ),
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            0x80 => {
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                    .unwrap_or(INBOX_FOLDER_ID);
                let mut partial_completion = false;
                for (message_id, unread) in request.import_read_state_changes() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-sync-import-read-state".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x80,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            0x93 => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource { state, .. })
                | Some(MapiObject::SynchronizationCollector { state, .. }) => {
                    state.extend_from_slice(request.local_replica_midset_deleted());
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x93,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            0x7F => {
                let (first_global_counter, count) = mapi_mailstore::local_replica_id_range(
                    principal.account_id,
                    request.local_replica_id_count(),
                    session.next_local_replica_sequence,
                );
                session.next_local_replica_sequence =
                    session.next_local_replica_sequence.saturating_add(1).max(1);
                responses.extend_from_slice(&rop_get_local_replica_ids_response(
                    &request,
                    first_global_counter,
                    count,
                ));
            }
            0x68 => responses.extend_from_slice(&rop_get_receive_folder_table_response(&request)),
            0x55 => responses
                .extend_from_slice(&rop_get_names_from_property_ids_response(&request, session)),
            0x56 => {
                let properties = match request.named_property_names() {
                    Ok(properties) => properties,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x56,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                if properties.is_empty()
                    && matches!(
                        input_object(session, &handle_slots, &request),
                        Some(MapiObject::Logon)
                    )
                {
                    let property_ids = session
                        .named_properties_for_query(None)
                        .into_iter()
                        .map(|(property_id, _property)| property_id)
                        .collect::<Vec<_>>();
                    responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                        &request,
                        &property_ids,
                    ));
                    continue;
                }
                let mut property_ids = Vec::with_capacity(properties.len());
                let mut exhausted = false;
                for property in properties {
                    match session.property_id_for_name(property, request.named_property_create()) {
                        Some(property_id) => property_ids.push(property_id),
                        None if request.named_property_create() => {
                            exhausted = true;
                            break;
                        }
                        None => property_ids.push(0),
                    }
                }
                if exhausted {
                    responses.extend_from_slice(&rop_error_response(
                        0x56,
                        request.response_handle_index(),
                        0x8007_000E,
                    ));
                } else {
                    responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                        &request,
                        &property_ids,
                    ));
                }
            }
            0x5F => {
                responses.extend_from_slice(&rop_query_named_properties_response(&request, session))
            }
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
const CONTACTS_FOLDER_ID: u64 = mapi_store_id(15);
const CALENDAR_FOLDER_ID: u64 = mapi_store_id(16);

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
const PID_TAG_RECIPIENT_TYPE: u32 = 0x0C15_0003;
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
const PID_TAG_FLAG_STATUS: u32 = 0x1090_0003;
const PID_TAG_LAST_MODIFICATION_TIME: u32 = 0x3008_0040;
const PID_TAG_SOURCE_KEY: u32 = 0x65E0_0102;
const PID_TAG_PARENT_SOURCE_KEY: u32 = 0x65E1_0102;
const PID_TAG_CHANGE_KEY: u32 = 0x65E2_0102;
const PID_TAG_PREDECESSOR_CHANGE_LIST: u32 = 0x65E3_0102;
const PID_TAG_MID: u32 = 0x674A_0014;
const PID_TAG_CHANGE_NUMBER: u32 = 0x67A4_0014;
const PID_TAG_ATTACH_DATA_BINARY: u32 = 0x3701_0102;
const PID_TAG_ATTACH_SIZE: u32 = 0x0E20_0003;
const PID_TAG_ATTACH_NUM: u32 = 0x0E21_0003;
const PID_TAG_ATTACH_FILENAME_W: u32 = 0x3704_001F;
const PID_TAG_ATTACH_METHOD: u32 = 0x3705_0003;
const PID_TAG_ATTACH_LONG_FILENAME_W: u32 = 0x3707_001F;
const PID_TAG_RENDERING_POSITION: u32 = 0x370B_0003;
const PID_TAG_ATTACH_MIME_TAG_W: u32 = 0x370E_001F;
const PID_TAG_EMAIL_ADDRESS_W: u32 = 0x3003_001F;
const PID_TAG_SMTP_ADDRESS_W: u32 = 0x39FE_001F;
const PID_TAG_GIVEN_NAME_W: u32 = 0x3A06_001F;
const PID_TAG_BUSINESS_TELEPHONE_NUMBER_W: u32 = 0x3A08_001F;
const PID_TAG_HOME_TELEPHONE_NUMBER_W: u32 = 0x3A09_001F;
const PID_TAG_SURNAME_W: u32 = 0x3A11_001F;
const PID_TAG_COMPANY_NAME_W: u32 = 0x3A16_001F;
const PID_TAG_TITLE_W: u32 = 0x3A17_001F;
const PID_TAG_MOBILE_TELEPHONE_NUMBER_W: u32 = 0x3A1C_001F;
const PID_TAG_START_DATE: u32 = 0x0060_0040;
const PID_TAG_END_DATE: u32 = 0x0061_0040;
const PID_TAG_LOCATION_W: u32 = 0x3FFB_001F;
const FIRST_NAMED_PROPERTY_ID: u16 = 0x8001;
const MAX_NAMED_PROPERTY_ID: u16 = 0xFFFE;
const PS_MAPI_GUID: [u8; 16] = [
    0x28, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];
const PS_INTERNET_HEADERS_GUID: [u8; 16] = [
    0x86, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];

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

    fn property_id_for_name(&mut self, property: MapiNamedProperty, create: bool) -> Option<u16> {
        let property = normalize_named_property(property);
        if property.guid == PS_MAPI_GUID {
            if let MapiNamedPropertyKind::Lid(lid) = &property.kind {
                return u16::try_from(*lid)
                    .ok()
                    .filter(|id| *id < FIRST_NAMED_PROPERTY_ID);
            }
        }
        if let Some(property_id) = self.named_properties.get(&property).copied() {
            return Some(property_id);
        }
        if !create || self.next_named_property_id > MAX_NAMED_PROPERTY_ID {
            return None;
        }

        let property_id = self.next_named_property_id;
        self.next_named_property_id = self.next_named_property_id.saturating_add(1);
        self.named_properties.insert(property.clone(), property_id);
        self.named_property_ids.insert(property_id, property);
        Some(property_id)
    }

    fn property_name_for_id(&self, property_id: u16) -> MapiNamedProperty {
        self.named_property_ids
            .get(&property_id)
            .cloned()
            .unwrap_or(MapiNamedProperty {
                guid: PS_MAPI_GUID,
                kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
            })
    }

    fn named_properties_for_query(&self, guid: Option<[u8; 16]>) -> Vec<(u16, MapiNamedProperty)> {
        let mut properties = self
            .named_property_ids
            .iter()
            .filter(|(_property_id, property)| match guid {
                Some(guid) => property.guid == guid,
                None => true,
            })
            .map(|(property_id, property)| (*property_id, property.clone()))
            .collect::<Vec<_>>();
        properties.sort_by_key(|(property_id, _property)| *property_id);
        properties
    }
}

fn normalize_named_property(mut property: MapiNamedProperty) -> MapiNamedProperty {
    if property.guid == PS_INTERNET_HEADERS_GUID {
        if let MapiNamedPropertyKind::Name(name) = property.kind {
            property.kind = MapiNamedPropertyKind::Name(name.to_ascii_lowercase());
        }
    }
    property
}

impl MapiObject {
    fn folder_id(&self) -> Option<u64> {
        match self {
            MapiObject::AttachmentStream { .. } => None,
            MapiObject::Logon => Some(ROOT_FOLDER_ID),
            MapiObject::Folder { folder_id }
            | MapiObject::Message { folder_id, .. }
            | MapiObject::Contact { folder_id, .. }
            | MapiObject::Event { folder_id, .. }
            | MapiObject::PendingMessage { folder_id, .. }
            | MapiObject::PendingContact { folder_id, .. }
            | MapiObject::PendingEvent { folder_id, .. }
            | MapiObject::HierarchyTable { folder_id, .. }
            | MapiObject::ContentsTable { folder_id, .. }
            | MapiObject::AttachmentTable { folder_id, .. }
            | MapiObject::Attachment { folder_id, .. }
            | MapiObject::PendingAttachment { folder_id, .. }
            | MapiObject::SavedAttachment { folder_id, .. }
            | MapiObject::SynchronizationSource { folder_id, .. }
            | MapiObject::SynchronizationCollector { folder_id, .. } => Some(*folder_id),
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

fn synchronization_context_state(object: Option<&MapiObject>) -> Option<(u64, Vec<u8>)> {
    match object {
        Some(MapiObject::SynchronizationSource {
            folder_id, state, ..
        })
        | Some(MapiObject::SynchronizationCollector {
            folder_id, state, ..
        }) => Some((*folder_id, state.clone())),
        _ => None,
    }
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

fn hierarchy_row_count(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    if is_root_hierarchy_folder(folder_id) {
        mailboxes
            .len()
            .saturating_add(snapshot.collaboration_folders().len())
            .min(u32::MAX as usize) as u32
    } else {
        0
    }
}

fn folder_message_count(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    if let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) {
        return folder.item_count;
    }
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
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
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
        PID_TAG_FLAG_STATUS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_BODY_W,
        PID_TAG_INTERNET_MESSAGE_ID_W,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

fn default_contact_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_GIVEN_NAME_W,
        PID_TAG_SURNAME_W,
        PID_TAG_EMAIL_ADDRESS_W,
        PID_TAG_SMTP_ADDRESS_W,
        PID_TAG_MOBILE_TELEPHONE_NUMBER_W,
        PID_TAG_BUSINESS_TELEPHONE_NUMBER_W,
        PID_TAG_COMPANY_NAME_W,
        PID_TAG_TITLE_W,
        PID_TAG_BODY_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

fn default_event_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_BODY_W,
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
        PID_TAG_LOCATION_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
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

fn rop_open_message_response(
    request: &RopRequest,
    subject: &str,
    recipient_count: usize,
) -> Vec<u8> {
    let mut response = vec![0x03, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    write_typed_string(&mut response, "");
    write_typed_string(&mut response, subject);
    response.extend_from_slice(&(recipient_count as u16).to_le_bytes());
    response.extend_from_slice(&0u16.to_le_bytes());
    response.push(0);
    response
}

fn rop_reload_cached_information_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<u8> {
    let (subject, recipient_count) = match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
        }) => match message_for_id(*folder_id, *message_id, mailboxes, emails) {
            Some(email) => (email.subject.clone(), message_recipients(email).len()),
            None => {
                return rop_error_response(0x10, request.response_handle_index(), 0x8004_010F);
            }
        },
        Some(MapiObject::PendingMessage {
            properties,
            recipients,
            ..
        }) => (
            pending_text_property(
                properties,
                &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
            ),
            recipients.len(),
        ),
        Some(MapiObject::PendingContact { properties, .. }) => (
            pending_text_property(
                properties,
                &[
                    PID_TAG_DISPLAY_NAME_W,
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                ],
            ),
            0,
        ),
        Some(MapiObject::PendingEvent { properties, .. }) => (
            pending_text_property(
                properties,
                &[
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                    PID_TAG_DISPLAY_NAME_W,
                ],
            ),
            0,
        ),
        _ => return rop_error_response(0x10, request.response_handle_index(), 0x0000_04B9),
    };

    let mut response = vec![0x10, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    write_typed_string(&mut response, "");
    write_typed_string(&mut response, &subject);
    response.extend_from_slice(&(recipient_count as u16).to_le_bytes());
    response.extend_from_slice(&0u16.to_le_bytes());
    response.push(0);
    response
}

fn rop_message_status_response(request: &RopRequest, old_status: u32) -> Vec<u8> {
    let mut response = vec![0x20, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, old_status);
    response
}

fn rop_create_folder_response(request: &RopRequest, folder_id: u64, existing: bool) -> Vec<u8> {
    let mut response = vec![0x1C, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u64(&mut response, folder_id);
    response.push(existing as u8);
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

fn rop_create_attachment_response(request: &RopRequest, attach_num: u32) -> Vec<u8> {
    let mut response = vec![0x23, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u32(&mut response, attach_num);
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
    let MapiObject::AttachmentStream { data, position, .. } = stream else {
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

fn rop_write_stream_response(request: &RopRequest, written: usize) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(written.min(u16::MAX as usize) as u16).to_le_bytes());
    response
}

fn rop_set_read_flags_response(request: &RopRequest, partial_completion: bool) -> Vec<u8> {
    let mut response = vec![0x66, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(partial_completion as u8);
    response
}

fn rop_synchronization_configure_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x70, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

fn rop_fast_transfer_source_get_buffer_response(
    request: &RopRequest,
    transfer_buffer: &[u8],
    transfer_position: &mut usize,
) -> Vec<u8> {
    let requested = request.fast_transfer_buffer_size().min(u16::MAX as usize);
    let end = transfer_position
        .saturating_add(requested)
        .min(transfer_buffer.len());
    let chunk = transfer_buffer[*transfer_position..end].to_vec();
    *transfer_position = end;
    let done = *transfer_position >= transfer_buffer.len();

    let mut response = vec![0x4E, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(if done { 0x0003u16 } else { 0x0001u16 }).to_le_bytes());
    response.extend_from_slice(&((*transfer_position).min(u16::MAX as usize) as u16).to_le_bytes());
    response
        .extend_from_slice(&(transfer_buffer.len().min(u16::MAX as usize) as u16).to_le_bytes());
    response.push(0);
    response.extend_from_slice(&(chunk.len().min(u16::MAX as usize) as u16).to_le_bytes());
    response.extend_from_slice(&chunk);
    response
}

fn rop_synchronization_get_transfer_state_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x82, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

fn rop_synchronization_import_message_change_response(
    request: &RopRequest,
    message_id: u64,
) -> Vec<u8> {
    let mut response = vec![0x72, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u64(&mut response, message_id);
    response
}

fn rop_synchronization_import_hierarchy_change_response(
    request: &RopRequest,
    folder_id: u64,
) -> Vec<u8> {
    let mut response = vec![0x73, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u64(&mut response, folder_id);
    response
}

fn rop_synchronization_import_message_move_response(
    request: &RopRequest,
    message_id: u64,
) -> Vec<u8> {
    let mut response = vec![0x78, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u64(&mut response, message_id);
    response
}

fn rop_get_local_replica_ids_response(
    request: &RopRequest,
    first_global_counter: u64,
    count: u32,
) -> Vec<u8> {
    let mut response = vec![0x7F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&mapi_mailstore::STORE_REPLICA_GUID);
    write_u64(&mut response, mapi_store_id(first_global_counter));
    write_u32(&mut response, count);
    response
}

fn rop_partial_completion_response(
    rop_id: u8,
    handle_index: u8,
    partial_completion: bool,
) -> Vec<u8> {
    let mut response = vec![rop_id, handle_index];
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

fn rop_create_message_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x06, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

fn rop_set_properties_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&0u16.to_le_bytes());
    response
}

fn rop_delete_properties_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&0u16.to_le_bytes());
    response
}

fn rop_simple_success_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

fn rop_save_changes_message_response(request: &RopRequest, message_id: u64) -> Vec<u8> {
    let mut response = vec![0x0C, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(request.input_handle_index().unwrap_or(0));
    write_u64(&mut response, message_id);
    response
}

fn rop_get_properties_list_response(request: &RopRequest, object: Option<&MapiObject>) -> Vec<u8> {
    let tags = match object {
        Some(
            MapiObject::Attachment { .. }
            | MapiObject::PendingAttachment { .. }
            | MapiObject::SavedAttachment { .. },
        ) => default_attachment_columns(),
        Some(MapiObject::Contact { .. }) | Some(MapiObject::PendingContact { .. }) => {
            default_contact_property_tags()
        }
        Some(MapiObject::Event { .. }) | Some(MapiObject::PendingEvent { .. }) => {
            default_event_property_tags()
        }
        Some(MapiObject::Message { .. }) | Some(MapiObject::PendingMessage { .. }) => {
            default_message_property_tags()
        }
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
    principal: &AccountPrincipal,
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
        Some(MapiObject::PendingMessage { properties, .. }) => {
            serialize_pending_message_row(principal, properties, &columns)
        }
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
        }) => {
            let Some(contact) = snapshot.contact_for_id(*folder_id, *contact_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_contact_row(&contact.contact, contact.id, contact.folder_id, &columns)
        }
        Some(MapiObject::PendingContact { properties, .. }) => {
            serialize_pending_contact_row(principal, properties, &columns)
        }
        Some(MapiObject::Event {
            folder_id,
            event_id,
        }) => {
            let Some(event) = snapshot.event_for_id(*folder_id, *event_id) else {
                return rop_error_response(
                    0x07,
                    request.input_handle_index().unwrap_or(0),
                    0x8004_010F,
                );
            };
            serialize_event_row(&event.event, event.id, event.folder_id, &columns)
        }
        Some(MapiObject::PendingEvent { properties, .. }) => {
            serialize_pending_event_row(principal, properties, &columns)
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
        Some(MapiObject::PendingAttachment {
            attach_num,
            properties,
            data,
            ..
        }) => serialize_pending_attachment_row(*attach_num, properties, data, &columns),
        Some(MapiObject::SavedAttachment {
            attach_num,
            file_reference,
            file_name,
            media_type,
            size_octets,
            ..
        }) => serialize_saved_attachment_row(
            *attach_num,
            file_reference,
            file_name,
            media_type,
            *size_octets,
            &columns,
        ),
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| serialize_folder_row(mailbox, &columns))
                .or_else(|| {
                    snapshot
                        .collaboration_folder_for_id(folder_id)
                        .map(|folder| serialize_collaboration_folder_row(folder, &columns))
                })
                .unwrap_or_else(|| serialize_root_folder_row(mailboxes, &columns))
        }
    };
    response.extend_from_slice(&row);
    response
}

fn rop_get_properties_all_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let mut response = vec![0x08, request.input_handle_index().unwrap_or(0)];
    write_u32(&mut response, 0);
    let tags = match object {
        Some(
            MapiObject::Attachment { .. }
            | MapiObject::PendingAttachment { .. }
            | MapiObject::SavedAttachment { .. },
        ) => default_attachment_columns(),
        Some(MapiObject::Message { .. }) | Some(MapiObject::PendingMessage { .. }) => {
            default_message_property_tags()
        }
        _ => default_folder_property_tags(),
    };
    response.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        write_u32(&mut response, tag);
        let value = serialize_object_property(object, principal, mailboxes, emails, snapshot, tag);
        response.extend_from_slice(&value);
    }
    response
}

fn rop_get_valid_attachments_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(MapiObject::Message {
        folder_id,
        message_id,
    }) = object
    else {
        return rop_error_response(0x52, request.response_handle_index(), 0x0000_04B9);
    };
    let attachments = snapshot
        .attachments_for_message(*folder_id, *message_id)
        .unwrap_or_default();
    let mut response = vec![0x52, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(attachments.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for attachment in attachments.iter().take(u16::MAX as usize) {
        write_u32(&mut response, attachment.attach_num);
    }
    response
}

fn rop_get_property_ids_from_names_response(request: &RopRequest, property_ids: &[u16]) -> Vec<u8> {
    let mut response = vec![0x56, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(property_ids.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for property_id in property_ids.iter().take(u16::MAX as usize) {
        response.extend_from_slice(&property_id.to_le_bytes());
    }
    response
}

fn rop_get_names_from_property_ids_response(
    request: &RopRequest,
    session: &MapiSession,
) -> Vec<u8> {
    let property_ids = request.property_ids();
    let mut response = vec![0x55, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(property_ids.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for property_id in property_ids.iter().take(u16::MAX as usize) {
        write_named_property(&mut response, &session.property_name_for_id(*property_id));
    }
    response
}

fn rop_query_named_properties_response(request: &RopRequest, session: &MapiSession) -> Vec<u8> {
    let properties = session.named_properties_for_query(request.named_property_query_guid());
    let mut response = vec![0x5F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(properties.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for (property_id, _property) in properties.iter().take(u16::MAX as usize) {
        response.extend_from_slice(&property_id.to_le_bytes());
    }
    for (_property_id, property) in properties.iter().take(u16::MAX as usize) {
        write_named_property(&mut response, property);
    }
    response
}

fn serialize_object_property(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
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
        Some(MapiObject::PendingMessage { properties, .. }) => {
            serialize_pending_message_row(principal, properties, &[tag])
        }
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
        }) => snapshot
            .contact_for_id(*folder_id, *contact_id)
            .map(|contact| {
                serialize_contact_row(&contact.contact, contact.id, contact.folder_id, &[tag])
            })
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingContact { properties, .. }) => {
            serialize_pending_contact_row(principal, properties, &[tag])
        }
        Some(MapiObject::Event {
            folder_id,
            event_id,
        }) => snapshot
            .event_for_id(*folder_id, *event_id)
            .map(|event| serialize_event_row(&event.event, event.id, event.folder_id, &[tag]))
            .unwrap_or_else(|| {
                let mut value = Vec::new();
                write_property_default(&mut value, tag);
                value
            }),
        Some(MapiObject::PendingEvent { properties, .. }) => {
            serialize_pending_event_row(principal, properties, &[tag])
        }
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
        Some(MapiObject::PendingAttachment {
            attach_num,
            properties,
            data,
            ..
        }) => serialize_pending_attachment_row(*attach_num, properties, data, &[tag]),
        Some(MapiObject::SavedAttachment {
            attach_num,
            file_reference,
            file_name,
            media_type,
            size_octets,
            ..
        }) => serialize_saved_attachment_row(
            *attach_num,
            file_reference,
            file_name,
            media_type,
            *size_octets,
            &[tag],
        ),
        _ => {
            let folder_id = object
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            folder_row_for_id(folder_id, mailboxes)
                .map(|mailbox| serialize_folder_row(mailbox, &[tag]))
                .or_else(|| {
                    snapshot
                        .collaboration_folder_for_id(folder_id)
                        .map(|folder| serialize_collaboration_folder_row(folder, &[tag]))
                })
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
    let start = request.row_id().unwrap_or(0) as usize;

    let mut response = vec![0x0F, input_handle_index];
    write_u32(&mut response, 0);

    match object {
        Some(MapiObject::Message {
            folder_id,
            message_id,
        }) => {
            let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) else {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            };
            let recipients = message_recipients(email);
            if start >= recipients.len() {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            }
            for (offset, recipient) in recipients.into_iter().enumerate().skip(start) {
                write_u32(&mut response, offset as u32);
                response.push(recipient.recipient_type);
                response.extend_from_slice(&0x0FFFu16.to_le_bytes());
                response.extend_from_slice(&0u16.to_le_bytes());
                let row = serialize_recipient_row(recipient.address);
                response.extend_from_slice(&(row.len() as u16).to_le_bytes());
                response.extend_from_slice(&row);
            }
        }
        Some(MapiObject::PendingMessage { recipients, .. }) => {
            if start >= recipients.len() {
                return rop_error_response(0x0F, input_handle_index, 0x8004_010F);
            }
            for recipient in recipients.iter().skip(start) {
                write_u32(&mut response, recipient.row_id);
                response.push(recipient.recipient_type);
                response.extend_from_slice(&0x0FFFu16.to_le_bytes());
                response.extend_from_slice(&0u16.to_le_bytes());
                let row = serialize_pending_recipient_row(recipient);
                response.extend_from_slice(&(row.len() as u16).to_le_bytes());
                response.extend_from_slice(&row);
            }
        }
        _ => return rop_error_response(0x0F, input_handle_index, 0x0000_04B9),
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
            let mut serialized = rows
                .into_iter()
                .map(|mailbox| serialize_folder_row(mailbox, &columns))
                .collect::<Vec<_>>();
            let mut collaboration_rows = snapshot
                .collaboration_folders()
                .iter()
                .filter(|folder| {
                    restriction_matches_collaboration_folder(restriction.as_ref(), folder)
                })
                .collect::<Vec<_>>();
            sort_collaboration_folders(&mut collaboration_rows, sort_orders);
            serialized.extend(
                collaboration_rows
                    .into_iter()
                    .map(|folder| serialize_collaboration_folder_row(folder, &columns)),
            );
            serialized
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
            if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                match folder.kind {
                    MapiCollaborationFolderKind::Contacts => {
                        let mut rows = snapshot.contacts_for_folder(*folder_id);
                        rows.retain(|contact| {
                            restriction_matches_contact(restriction.as_ref(), &contact.contact)
                        });
                        sort_contacts(&mut rows, sort_orders);
                        rows.into_iter()
                            .map(|contact| {
                                serialize_contact_row(
                                    &contact.contact,
                                    contact.id,
                                    contact.folder_id,
                                    &columns,
                                )
                            })
                            .collect::<Vec<_>>()
                    }
                    MapiCollaborationFolderKind::Calendar => {
                        let mut rows = snapshot.events_for_folder(*folder_id);
                        rows.retain(|event| {
                            restriction_matches_event(restriction.as_ref(), &event.event)
                        });
                        sort_events(&mut rows, sort_orders);
                        rows.into_iter()
                            .map(|event| {
                                serialize_event_row(
                                    &event.event,
                                    event.id,
                                    event.folder_id,
                                    &columns,
                                )
                            })
                            .collect::<Vec<_>>()
                    }
                }
            } else {
                let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                rows.retain(|email| restriction_matches_email(restriction.as_ref(), email));
                sort_emails(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|email| serialize_message_row(email, &columns))
                    .collect::<Vec<_>>()
            }
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

fn sort_collaboration_folders(
    rows: &mut [&MapiCollaborationFolder],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_DISPLAY_NAME_W => compare_case_insensitive(
                    &left.collection.display_name,
                    &right.collection.display_name,
                ),
                PID_TAG_CONTENT_COUNT => left.item_count.cmp(&right.item_count),
                PID_TAG_FOLDER_ID => left.id.cmp(&right.id),
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

fn sort_contacts(rows: &mut [&crate::mapi_store::MapiContact], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_DISPLAY_NAME_W | PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.contact.name, &right.contact.name)
                }
                PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
                    compare_case_insensitive(&left.contact.email, &right.contact.email)
                }
                PID_TAG_COMPANY_NAME_W => {
                    compare_case_insensitive(&left.contact.team, &right.contact.team)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
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

fn sort_events(rows: &mut [&crate::mapi_store::MapiEvent], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.event.title, &right.event.title)
                }
                PID_TAG_START_DATE
                | PID_TAG_MESSAGE_DELIVERY_TIME
                | PID_TAG_LAST_MODIFICATION_TIME => {
                    event_start_sort_key(&left.event).cmp(&event_start_sort_key(&right.event))
                }
                PID_TAG_LOCATION_W => {
                    compare_case_insensitive(&left.event.location, &right.event.location)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
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

fn restriction_matches_collaboration_folder(
    restriction: Option<&MapiRestriction>,
    folder: &MapiCollaborationFolder,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        collaboration_folder_property_value(folder, property_tag)
    })
}

fn restriction_matches_email(restriction: Option<&MapiRestriction>, email: &JmapEmail) -> bool {
    restriction_matches(restriction, |property_tag| {
        email_property_value(email, property_tag)
    })
}

fn restriction_matches_contact(
    restriction: Option<&MapiRestriction>,
    contact: &AccessibleContact,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        contact_property_value(
            contact,
            mapi_item_id(&contact.id),
            CONTACTS_FOLDER_ID,
            property_tag,
        )
    })
}

fn restriction_matches_event(
    restriction: Option<&MapiRestriction>,
    event: &AccessibleEvent,
) -> bool {
    restriction_matches(restriction, |property_tag| {
        event_property_value(
            event,
            mapi_item_id(&event.id),
            CALENDAR_FOLDER_ID,
            property_tag,
        )
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
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &mailbox.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::canonical_folder_change_number(mailbox),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::canonical_folder_change_number(mailbox),
            )))
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(
            mapi_mailstore::canonical_folder_change_number(mailbox),
        )),
        _ => None,
    }
}

fn collaboration_folder_property_value(
    folder: &MapiCollaborationFolder,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String(folder.collection.display_name.clone())),
        PID_TAG_CONTENT_COUNT => Some(MapiValue::U32(folder.item_count)),
        PID_TAG_CONTENT_UNREAD_COUNT => Some(MapiValue::U32(0)),
        PID_TAG_SUBFOLDERS => Some(MapiValue::Bool(false)),
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder.id)),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String(
            collaboration_folder_message_class(folder.kind).to_string(),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            folder.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(folder.id),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(folder.id),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(folder.id)),
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
        PID_TAG_FLAG_STATUS => Some(MapiValue::U32(if email.flagged { 2 } else { 0 })),
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
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &email.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &email.mailbox_id,
        ))),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::canonical_message_change_number(email),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::canonical_message_change_number(email),
            )))
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(
            mapi_mailstore::canonical_message_change_number(email),
        )),
        PID_TAG_INTERNET_MESSAGE_ID_W => email.internet_message_id.clone().map(MapiValue::String),
        _ => None,
    }
}

fn contact_property_value(
    contact: &AccessibleContact,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_DISPLAY_NAME_W | PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(contact.name.clone()))
        }
        PID_TAG_GIVEN_NAME_W => contact
            .name
            .split_whitespace()
            .next()
            .map(|value| MapiValue::String(value.to_string())),
        PID_TAG_SURNAME_W => contact
            .name
            .split_whitespace()
            .last()
            .filter(|value| *value != contact.name)
            .map(|value| MapiValue::String(value.to_string())),
        PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
            Some(MapiValue::String(contact.email.clone()))
        }
        PID_TAG_MOBILE_TELEPHONE_NUMBER_W
        | PID_TAG_BUSINESS_TELEPHONE_NUMBER_W
        | PID_TAG_HOME_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact.phone.clone())),
        PID_TAG_COMPANY_NAME_W => Some(MapiValue::String(contact.team.clone())),
        PID_TAG_TITLE_W => Some(MapiValue::String(contact.role.clone())),
        PID_TAG_BODY_W => Some(MapiValue::String(contact.notes.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Contact".to_string())),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0x0000_0001)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(contact_size(contact))),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
            Some(MapiValue::Binary(contact.id.as_bytes().to_vec()))
        }
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &contact.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(item_id),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(item_id),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(item_id)),
        _ => None,
    }
}

fn event_property_value(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    match property_tag {
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
            Some(MapiValue::String(event.title.clone()))
        }
        PID_TAG_BODY_W => Some(MapiValue::String(event.notes.clone())),
        PID_TAG_START_DATE | PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
            Some(MapiValue::I64(event_start_filetime(event) as i64))
        }
        PID_TAG_END_DATE => Some(MapiValue::I64(event_end_filetime(event) as i64)),
        PID_TAG_LOCATION_W => Some(MapiValue::String(event.location.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Appointment".to_string())),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0x0000_0001)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(event_size(event))),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
            Some(MapiValue::Binary(event.id.as_bytes().to_vec()))
        }
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &event.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(item_id),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(item_id),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(item_id)),
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

fn next_pending_attachment_num(
    session: &MapiSession,
    folder_id: u64,
    message_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    let snapshot_max = snapshot
        .attachments_for_message(folder_id, message_id)
        .unwrap_or_default()
        .iter()
        .map(|attachment| attachment.attach_num)
        .max();
    let session_max = session
        .handles
        .values()
        .filter_map(|object| match object {
            MapiObject::PendingAttachment {
                folder_id: pending_folder_id,
                message_id: pending_message_id,
                attach_num,
                ..
            }
            | MapiObject::SavedAttachment {
                folder_id: pending_folder_id,
                message_id: pending_message_id,
                attach_num,
                ..
            } if *pending_folder_id == folder_id && *pending_message_id == message_id => {
                Some(*attach_num)
            }
            _ => None,
        })
        .max();
    snapshot_max
        .into_iter()
        .chain(session_max)
        .max()
        .map(|value| value.saturating_add(1))
        .unwrap_or(0)
}

async fn attachment_stream_data<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    input_handle: u32,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(Vec<u8>, Option<u32>)> {
    match session.handles.get(&input_handle)?.clone() {
        MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        } => {
            let attachment = snapshot.attachment_for_message(folder_id, message_id, attach_num)?;
            let content = store
                .fetch_attachment_content(principal.account_id, &attachment.file_reference)
                .await
                .ok()??;
            Some((content.blob_bytes, None))
        }
        MapiObject::PendingAttachment { data, .. } => Some((data, Some(input_handle))),
        MapiObject::SavedAttachment { file_reference, .. } => {
            let content = store
                .fetch_attachment_content(principal.account_id, &file_reference)
                .await
                .ok()??;
            Some((content.blob_bytes, None))
        }
        _ => None,
    }
}

fn write_attachment_stream(
    session: &mut MapiSession,
    stream_handle: u32,
    bytes: &[u8],
) -> Option<usize> {
    let (updated_data, writable_attachment_handle, written) = {
        let Some(MapiObject::AttachmentStream {
            data,
            position,
            writable_attachment_handle,
        }) = session.handles.get_mut(&stream_handle)
        else {
            return None;
        };
        let start = *position;
        let end = start.checked_add(bytes.len())?;
        if data.len() < end {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(bytes);
        *position = end;
        (data.clone(), *writable_attachment_handle, bytes.len())
    };

    if let Some(attachment_handle) = writable_attachment_handle {
        if let Some(MapiObject::PendingAttachment { data, .. }) =
            session.handles.get_mut(&attachment_handle)
        {
            *data = updated_data;
        }
    }

    Some(written)
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

fn serialize_pending_attachment_row(
    attach_num: u32,
    properties: &HashMap<u32, MapiValue>,
    data: &[u8],
    columns: &[u32],
) -> Vec<u8> {
    let file_name = pending_attachment_file_name(attach_num, properties);
    let media_type = pending_attachment_media_type(properties);
    let size = data.len().min(u32::MAX as usize) as u32;
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) = properties.get(column) {
            write_mapi_value(&mut row, *column, value);
            continue;
        }
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &file_name)
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size),
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, 1),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ATTACH_DATA_BINARY => write_u16_prefixed_bytes(&mut row, data),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn serialize_saved_attachment_row(
    attach_num: u32,
    file_reference: &str,
    file_name: &str,
    media_type: &str,
    size_octets: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, file_name)
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size_octets.min(u32::MAX as u64) as u32),
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, 1),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
                write_u16_prefixed_bytes(&mut row, file_reference.as_bytes())
            }
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
            _ => match mailbox_property_value(mailbox, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

fn serialize_collaboration_folder_row(
    folder: &MapiCollaborationFolder,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &folder.collection.display_name),
            PID_TAG_FOLDER_ID => write_u64(&mut row, folder.id),
            PID_TAG_PARENT_FOLDER_ID => write_u64(&mut row, ROOT_FOLDER_ID),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, folder.item_count),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            PID_TAG_SUBFOLDERS => row.push(0),
            PID_TAG_MESSAGE_CLASS_W => {
                write_utf16z(&mut row, collaboration_folder_message_class(folder.kind))
            }
            PID_TAG_LAST_MODIFICATION_TIME => write_u64(&mut row, 0),
            _ => match collaboration_folder_property_value(folder, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
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
            _ => match email_property_value(email, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

fn serialize_contact_row(
    contact: &AccessibleContact,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match contact_property_value(contact, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

fn serialize_event_row(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match event_property_value(event, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

fn serialize_pending_message_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) = pending_message_property_value(principal, properties, *column) {
            write_mapi_value(&mut row, *column, &value);
        } else {
            write_property_default(&mut row, *column);
        }
    }
    row
}

fn pending_message_property_value(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Option<MapiValue> {
    properties
        .get(&property_tag)
        .cloned()
        .or_else(|| match property_tag {
            PID_TAG_NORMALIZED_SUBJECT_W => properties.get(&PID_TAG_SUBJECT_W).cloned(),
            PID_TAG_SUBJECT_W => properties.get(&PID_TAG_NORMALIZED_SUBJECT_W).cloned(),
            PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Note".to_string())),
            PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0x0000_0008)),
            PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
            PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(pending_message_size(properties))),
            PID_TAG_SENDER_NAME_W => Some(MapiValue::String(principal.display_name.clone())),
            PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(principal.email.clone())),
            _ => None,
        })
}

fn serialize_pending_contact_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let contact = contact_input_from_mapi(
        principal.account_id,
        None,
        &default_contact_for_mapping(principal.account_id, "default"),
        properties,
    );
    let contact = AccessibleContact {
        id: Uuid::nil(),
        collection_id: "default".to_string(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        rights: default_mapping_rights(),
        name: contact.name,
        role: contact.role,
        email: contact.email,
        phone: contact.phone,
        team: contact.team,
        notes: contact.notes,
    };
    serialize_contact_row(&contact, 0, CONTACTS_FOLDER_ID, columns)
}

fn serialize_pending_event_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let event = event_input_from_mapi(
        principal.account_id,
        None,
        &default_event_for_mapping(principal.account_id, "default"),
        properties,
    )
    .unwrap_or_else(|_| default_event_input(principal.account_id, None));
    let event = AccessibleEvent {
        id: Uuid::nil(),
        collection_id: "default".to_string(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        rights: default_mapping_rights(),
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
    };
    serialize_event_row(&event, 0, CALENDAR_FOLDER_ID, columns)
}

fn pending_message_size(properties: &HashMap<u32, MapiValue>) -> i64 {
    let subject = pending_text_property(
        properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    );
    let body = pending_text_property(properties, &[PID_TAG_BODY_W]);
    subject
        .len()
        .saturating_add(body.len())
        .min(i64::MAX as usize) as i64
}

fn pending_text_property(properties: &HashMap<u32, MapiValue>, tags: &[u32]) -> String {
    tags.iter()
        .find_map(|tag| {
            properties
                .get(tag)
                .and_then(|value| value.clone().into_text())
        })
        .unwrap_or_default()
}

fn optional_pending_text_property(
    properties: &HashMap<u32, MapiValue>,
    tags: &[u32],
) -> Option<String> {
    tags.iter()
        .find_map(|tag| {
            properties
                .get(tag)
                .and_then(|value| value.clone().into_text())
        })
        .filter(|value| !value.trim().is_empty())
}

fn default_mapping_rights() -> CollaborationRights {
    CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: false,
    }
}

fn default_contact_for_mapping(account_id: Uuid, collection_id: &str) -> AccessibleContact {
    AccessibleContact {
        id: Uuid::nil(),
        collection_id: collection_id.to_string(),
        owner_account_id: account_id,
        owner_email: String::new(),
        owner_display_name: String::new(),
        rights: default_mapping_rights(),
        name: String::new(),
        role: String::new(),
        email: String::new(),
        phone: String::new(),
        team: String::new(),
        notes: String::new(),
    }
}

fn default_event_for_mapping(account_id: Uuid, collection_id: &str) -> AccessibleEvent {
    AccessibleEvent {
        id: Uuid::nil(),
        collection_id: collection_id.to_string(),
        owner_account_id: account_id,
        owner_email: String::new(),
        owner_display_name: String::new(),
        rights: default_mapping_rights(),
        date: "1970-01-01".to_string(),
        time: "00:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 0,
        recurrence_rule: String::new(),
        title: String::new(),
        location: String::new(),
        attendees: String::new(),
        attendees_json: serialize_calendar_participants_metadata(
            &CalendarParticipantsMetadata::default(),
        ),
        notes: String::new(),
    }
}

fn default_event_input(account_id: Uuid, id: Option<Uuid>) -> UpsertClientEventInput {
    UpsertClientEventInput {
        id,
        account_id,
        date: "1970-01-01".to_string(),
        time: "00:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 0,
        recurrence_rule: String::new(),
        title: String::new(),
        location: String::new(),
        attendees: String::new(),
        attendees_json: serialize_calendar_participants_metadata(
            &CalendarParticipantsMetadata::default(),
        ),
        notes: String::new(),
    }
}

fn contact_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &AccessibleContact,
    properties: &HashMap<u32, MapiValue>,
) -> UpsertClientContactInput {
    let name = optional_pending_text_property(
        properties,
        &[
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_NORMALIZED_SUBJECT_W,
        ],
    )
    .or_else(|| {
        let given = optional_pending_text_property(properties, &[PID_TAG_GIVEN_NAME_W]);
        let surname = optional_pending_text_property(properties, &[PID_TAG_SURNAME_W]);
        match (given, surname) {
            (Some(given), Some(surname)) => Some(format!("{given} {surname}")),
            (Some(given), None) => Some(given),
            (None, Some(surname)) => Some(surname),
            (None, None) => None,
        }
    })
    .unwrap_or_else(|| existing.name.clone());
    UpsertClientContactInput {
        id,
        account_id,
        name,
        role: optional_pending_text_property(properties, &[PID_TAG_TITLE_W])
            .unwrap_or_else(|| existing.role.clone()),
        email: optional_pending_text_property(
            properties,
            &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W],
        )
        .unwrap_or_else(|| existing.email.clone()),
        phone: optional_pending_text_property(
            properties,
            &[
                PID_TAG_MOBILE_TELEPHONE_NUMBER_W,
                PID_TAG_BUSINESS_TELEPHONE_NUMBER_W,
                PID_TAG_HOME_TELEPHONE_NUMBER_W,
            ],
        )
        .unwrap_or_else(|| existing.phone.clone()),
        team: optional_pending_text_property(properties, &[PID_TAG_COMPANY_NAME_W])
            .unwrap_or_else(|| existing.team.clone()),
        notes: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.notes.clone()),
    }
}

fn event_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &AccessibleEvent,
    properties: &HashMap<u32, MapiValue>,
) -> Result<UpsertClientEventInput> {
    reject_unsupported_mapi_event_properties(properties)?;
    let start = properties
        .get(&PID_TAG_START_DATE)
        .and_then(MapiValue::as_i64)
        .and_then(filetime_to_date_time)
        .unwrap_or_else(|| (existing.date.clone(), existing.time.clone()));
    let end = properties
        .get(&PID_TAG_END_DATE)
        .and_then(MapiValue::as_i64)
        .and_then(filetime_to_date_time);
    let duration_minutes = match (
        properties
            .get(&PID_TAG_START_DATE)
            .and_then(MapiValue::as_i64),
        properties
            .get(&PID_TAG_END_DATE)
            .and_then(MapiValue::as_i64),
    ) {
        (Some(start), Some(end)) if end >= start => {
            ((end - start) / 10_000_000 / 60).clamp(0, i64::from(i32::MAX)) as i32
        }
        _ => existing.duration_minutes,
    };
    let (date, time) = start;
    Ok(UpsertClientEventInput {
        id,
        account_id,
        date,
        time,
        time_zone: existing.time_zone.clone(),
        duration_minutes: end
            .map(|_| duration_minutes)
            .unwrap_or(existing.duration_minutes),
        recurrence_rule: existing.recurrence_rule.clone(),
        title: optional_pending_text_property(
            properties,
            &[
                PID_TAG_SUBJECT_W,
                PID_TAG_NORMALIZED_SUBJECT_W,
                PID_TAG_DISPLAY_NAME_W,
            ],
        )
        .unwrap_or_else(|| existing.title.clone()),
        location: optional_pending_text_property(properties, &[PID_TAG_LOCATION_W])
            .unwrap_or_else(|| existing.location.clone()),
        attendees: existing.attendees.clone(),
        attendees_json: if existing.attendees_json.trim().is_empty() {
            serialize_calendar_participants_metadata(&CalendarParticipantsMetadata::default())
        } else {
            existing.attendees_json.clone()
        },
        notes: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.notes.clone()),
    })
}

fn reject_unsupported_mapi_event_properties(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    for (tag, value) in properties {
        let supported = matches!(
            *tag,
            PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_W
                | PID_TAG_BODY_W
                | PID_TAG_START_DATE
                | PID_TAG_END_DATE
                | PID_TAG_LOCATION_W
                | PID_TAG_MESSAGE_CLASS_W
        );
        if !supported {
            return Err(anyhow!(
                "MAPI calendar property {tag:#010X} is outside the canonical calendar subset"
            ));
        }
        if matches!(value, MapiValue::Binary(_)) {
            return Err(anyhow!(
                "MAPI binary calendar recurrence or meeting payloads are not supported"
            ));
        }
    }
    Ok(())
}

fn pending_attachment_upload(
    attach_num: u32,
    properties: &HashMap<u32, MapiValue>,
    data: Vec<u8>,
) -> AttachmentUploadInput {
    AttachmentUploadInput {
        file_name: pending_attachment_file_name(attach_num, properties),
        media_type: pending_attachment_media_type(properties),
        blob_bytes: data,
    }
}

fn pending_attachment_file_name(attach_num: u32, properties: &HashMap<u32, MapiValue>) -> String {
    optional_pending_text_property(
        properties,
        &[PID_TAG_ATTACH_LONG_FILENAME_W, PID_TAG_ATTACH_FILENAME_W],
    )
    .unwrap_or_else(|| format!("mapi-attachment-{attach_num}.bin"))
}

fn pending_attachment_media_type(properties: &HashMap<u32, MapiValue>) -> String {
    optional_pending_text_property(properties, &[PID_TAG_ATTACH_MIME_TAG_W])
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

fn mapi_expected_attachment_kind(media_type: &str, file_name: &str) -> ExpectedKind {
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

fn jmap_import_from_pending_message(
    principal: &AccountPrincipal,
    mailbox: &JmapMailbox,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) -> JmapImportedEmailInput {
    let subject = pending_text_property(
        properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    );
    let body_text = pending_text_property(properties, &[PID_TAG_BODY_W]);
    let from_address =
        optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
            .unwrap_or_else(|| principal.email.clone());
    let from_display = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .or_else(|| Some(principal.display_name.clone()));
    let internet_message_id =
        optional_pending_text_property(properties, &[PID_TAG_INTERNET_MESSAGE_ID_W]);
    let size_octets = subject
        .len()
        .saturating_add(body_text.len())
        .min(i64::MAX as usize) as i64;
    let (to, cc, bcc) = pending_recipients_for_import(recipients);

    JmapImportedEmailInput {
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        mailbox_id: mailbox.id,
        source: "mapi-save-message".to_string(),
        from_display,
        from_address,
        sender_display: None,
        sender_address: None,
        to,
        cc,
        bcc,
        subject,
        body_text,
        body_html_sanitized: None,
        internet_message_id,
        mime_blob_ref: format!("mapi-save-message:{}", Uuid::new_v4()),
        size_octets,
        received_at: None,
        attachments: Vec::new(),
    }
}

fn pending_recipients_for_import(
    recipients: &[PendingRecipient],
) -> (
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
) {
    let mut to = Vec::new();
    let mut cc = Vec::new();
    let mut bcc = Vec::new();
    for recipient in recipients {
        let input = SubmittedRecipientInput {
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
        };
        match recipient.recipient_type {
            0x02 => cc.push(input),
            0x03 => bcc.push(input),
            _ => to.push(input),
        }
    }
    (to, cc, bcc)
}

fn mapi_submit_from_pending_message(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) -> SubmitMessageInput {
    let subject = pending_text_property(
        properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    );
    let body_text = pending_text_property(properties, &[PID_TAG_BODY_W]);
    let from_address =
        optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
            .unwrap_or_else(|| principal.email.clone());
    let from_display = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .or_else(|| Some(principal.display_name.clone()));
    let internet_message_id =
        optional_pending_text_property(properties, &[PID_TAG_INTERNET_MESSAGE_ID_W]);
    let (to, cc, bcc) = pending_recipients_for_import(recipients);

    SubmitMessageInput {
        draft_message_id: None,
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        source: "mapi-submit-message".to_string(),
        from_display,
        from_address,
        sender_display: None,
        sender_address: None,
        to,
        cc,
        bcc,
        subject,
        body_text,
        body_html_sanitized: None,
        internet_message_id,
        mime_blob_ref: Some(format!("mapi-submit-message:{}", Uuid::new_v4())),
        size_octets: pending_message_size(properties),
        unread: Some(false),
        flagged: Some(false),
        attachments: Vec::new(),
    }
}

fn mapi_submit_from_email(principal: &AccountPrincipal, email: &JmapEmail) -> SubmitMessageInput {
    SubmitMessageInput {
        draft_message_id: Some(email.id),
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        source: "mapi-submit-message".to_string(),
        from_display: email.from_display.clone(),
        from_address: email.from_address.clone(),
        sender_display: email.sender_display.clone(),
        sender_address: email.sender_address.clone(),
        to: submitted_recipients_from_addresses(&email.to),
        cc: submitted_recipients_from_addresses(&email.cc),
        bcc: submitted_recipients_from_addresses(&email.bcc),
        subject: email.subject.clone(),
        body_text: email.body_text.clone(),
        body_html_sanitized: email.body_html_sanitized.clone(),
        internet_message_id: email.internet_message_id.clone(),
        mime_blob_ref: email.mime_blob_ref.clone(),
        size_octets: i64::try_from(email.size_octets).unwrap_or(i64::MAX),
        unread: Some(email.unread),
        flagged: Some(email.flagged),
        attachments: Vec::new(),
    }
}

fn submitted_recipients_from_addresses(
    addresses: &[JmapEmailAddress],
) -> Vec<SubmittedRecipientInput> {
    addresses
        .iter()
        .map(|address| SubmittedRecipientInput {
            address: address.address.clone(),
            display_name: address.display_name.clone(),
        })
        .collect()
}

fn submitted_mapi_folder_id(submitted: &SubmittedMessage, mailboxes: &[JmapMailbox]) -> u64 {
    mailboxes
        .iter()
        .find(|mailbox| mailbox.id == submitted.sent_mailbox_id)
        .map(mapi_folder_id)
        .unwrap_or(SENT_FOLDER_ID)
}

fn apply_pending_recipient_changes(
    recipients: &mut Vec<PendingRecipient>,
    changes: Vec<PendingRecipientChange>,
) {
    for change in changes {
        match change {
            PendingRecipientChange::Delete(row_id) => {
                recipients.retain(|recipient| recipient.row_id != row_id);
            }
            PendingRecipientChange::Upsert(recipient) => {
                if let Some(existing) = recipients
                    .iter_mut()
                    .find(|existing| existing.row_id == recipient.row_id)
                {
                    *existing = recipient;
                } else {
                    recipients.push(recipient);
                }
            }
        }
    }
    recipients.sort_by_key(|recipient| recipient.row_id);
}

fn hierarchy_display_name(
    hierarchy_values: &[(u32, MapiValue)],
    property_values: &[(u32, MapiValue)],
) -> Option<String> {
    hierarchy_values
        .iter()
        .chain(property_values.iter())
        .rev()
        .find_map(|(tag, value)| {
            (*tag == PID_TAG_DISPLAY_NAME_W)
                .then(|| value.as_text().map(str::trim).map(str::to_string))
                .flatten()
        })
        .filter(|value| !value.is_empty())
}

fn imported_hierarchy_existing_mailbox<'a>(
    hierarchy_values: &[(u32, MapiValue)],
    display_name: &str,
    mailboxes: &'a [JmapMailbox],
) -> Option<&'a JmapMailbox> {
    let source_key = hierarchy_values
        .iter()
        .find_map(|(tag, value)| match (tag, value) {
            (tag, MapiValue::Binary(value)) if *tag == PID_TAG_SOURCE_KEY => Some(value.as_slice()),
            _ => None,
        });
    if let Some(source_key) = source_key {
        if let Some(mailbox) = mailboxes
            .iter()
            .find(|mailbox| mapi_mailstore::source_key_for_uuid(&mailbox.id) == source_key)
        {
            return Some(mailbox);
        }
    }

    mailboxes
        .iter()
        .find(|mailbox| mailbox.name.eq_ignore_ascii_case(display_name))
}

fn system_folder_display_name(display_name: &str) -> bool {
    matches!(
        display_name.trim().to_ascii_lowercase().as_str(),
        "inbox"
            | "drafts"
            | "sent"
            | "sent items"
            | "deleted"
            | "deleted items"
            | "trash"
            | "outbox"
    )
}

async fn apply_canonical_message_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    message_id: u64,
    values: Vec<(u32, MapiValue)>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let email = message_for_id(folder_id, message_id, mailboxes, emails)
        .ok_or_else(|| anyhow!("canonical MAPI message was not found"))?;
    let mut unread = None;
    let mut flagged = None;

    for (tag, value) in values {
        match tag {
            PID_TAG_MESSAGE_FLAGS => {
                let flags = value
                    .into_u32()
                    .ok_or_else(|| anyhow!("invalid PidTagMessageFlags value"))?;
                unread = Some(flags & 0x0000_0001 == 0);
            }
            PID_TAG_FLAG_STATUS => {
                flagged = Some(
                    value
                        .as_i64()
                        .ok_or_else(|| anyhow!("invalid PidTagFlagStatus value"))?
                        != 0,
                );
            }
            _ => return Err(anyhow!("canonical MAPI message property is not mutable")),
        }
    }

    if unread.is_none() && flagged.is_none() {
        return Ok(());
    }

    store
        .update_jmap_email_flags(
            principal.account_id,
            email.id,
            unread,
            flagged,
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-set-message-properties".to_string(),
                subject: format!("message:{}", email.id),
            },
        )
        .await?;
    Ok(())
}

async fn apply_canonical_contact_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    contact_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let contact = snapshot
        .contact_for_id(folder_id, contact_id)
        .ok_or_else(|| anyhow!("canonical MAPI contact was not found"))?;
    let properties = values.into_iter().collect::<HashMap<_, _>>();
    let input = contact_input_from_mapi(
        principal.account_id,
        Some(contact.canonical_id),
        &contact.contact,
        &properties,
    );
    store
        .update_accessible_contact(principal.account_id, contact.canonical_id, input)
        .await?;
    Ok(())
}

async fn apply_canonical_event_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    event_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let event = snapshot
        .event_for_id(folder_id, event_id)
        .ok_or_else(|| anyhow!("canonical MAPI calendar event was not found"))?;
    let properties = values.into_iter().collect::<HashMap<_, _>>();
    let input = event_input_from_mapi(
        principal.account_id,
        Some(event.canonical_id),
        &event.event,
        &properties,
    )?;
    store
        .update_accessible_event(principal.account_id, event.canonical_id, input)
        .await?;
    Ok(())
}

fn apply_mapi_property_values(
    object: Option<&mut MapiObject>,
    values: Vec<(u32, MapiValue)>,
) -> Result<()> {
    match object {
        Some(MapiObject::PendingMessage { properties, .. }) => {
            properties.extend(values);
            Ok(())
        }
        Some(MapiObject::PendingContact { properties, .. })
        | Some(MapiObject::PendingEvent { properties, .. }) => {
            properties.extend(values);
            Ok(())
        }
        Some(MapiObject::PendingAttachment {
            properties, data, ..
        }) => {
            for (tag, value) in values {
                if tag == PID_TAG_ATTACH_DATA_BINARY {
                    if let MapiValue::Binary(bytes) = &value {
                        *data = bytes.clone();
                    }
                }
                properties.insert(tag, value);
            }
            Ok(())
        }
        _ => Err(anyhow!("MAPI object does not support property mutation")),
    }
}

fn delete_mapi_properties(object: Option<&mut MapiObject>, property_tags: &[u32]) -> Result<()> {
    match object {
        Some(MapiObject::PendingMessage { properties, .. }) => {
            for tag in property_tags {
                properties.remove(tag);
            }
            Ok(())
        }
        Some(MapiObject::PendingContact { properties, .. })
        | Some(MapiObject::PendingEvent { properties, .. }) => {
            for tag in property_tags {
                properties.remove(tag);
            }
            Ok(())
        }
        Some(MapiObject::PendingAttachment {
            properties, data, ..
        }) => {
            for tag in property_tags {
                properties.remove(tag);
                if *tag == PID_TAG_ATTACH_DATA_BINARY {
                    data.clear();
                }
            }
            Ok(())
        }
        _ => Err(anyhow!("MAPI object does not support property deletion")),
    }
}

fn write_mapi_value(row: &mut Vec<u8>, property_tag: u32, value: &MapiValue) {
    match property_tag & 0xFFFF {
        0x0003 => write_u32(row, value.clone().into_u32().unwrap_or_default()),
        0x000B => row.push(value.as_bool().unwrap_or_default() as u8),
        0x0014 => write_u64(row, value.as_i64().unwrap_or_default().max(0) as u64),
        0x001E | 0x001F => write_utf16z(row, &value.clone().into_text().unwrap_or_default()),
        0x0040 => write_u64(row, value.as_i64().unwrap_or_default().max(0) as u64),
        0x0102 => match value {
            MapiValue::Binary(bytes) => write_u16_prefixed_bytes(row, bytes),
            _ => write_u16_prefixed_bytes(row, &[]),
        },
        _ => write_property_default(row, property_tag),
    }
}

fn write_named_property(row: &mut Vec<u8>, property: &MapiNamedProperty) {
    match &property.kind {
        MapiNamedPropertyKind::Lid(lid) => {
            row.push(0x00);
            row.extend_from_slice(&property.guid);
            write_u32(row, *lid);
        }
        MapiNamedPropertyKind::Name(name) => {
            row.push(0x01);
            row.extend_from_slice(&property.guid);
            let mut name_bytes = name
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>();
            name_bytes.extend_from_slice(&0u16.to_le_bytes());
            let size = name_bytes.len().min(u8::MAX as usize);
            row.push(size as u8);
            row.extend_from_slice(&name_bytes[..size]);
        }
    }
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

fn serialize_pending_recipient_row(recipient: &PendingRecipient) -> Vec<u8> {
    let address = JmapEmailAddress {
        address: recipient.address.clone(),
        display_name: recipient.display_name.clone(),
    };
    serialize_recipient_row(&address)
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

fn contact_size(contact: &AccessibleContact) -> i64 {
    contact
        .name
        .len()
        .saturating_add(contact.email.len())
        .saturating_add(contact.phone.len())
        .saturating_add(contact.team.len())
        .saturating_add(contact.notes.len())
        .min(i64::MAX as usize) as i64
}

fn event_size(event: &AccessibleEvent) -> i64 {
    event
        .title
        .len()
        .saturating_add(event.location.len())
        .saturating_add(event.notes.len())
        .min(i64::MAX as usize) as i64
}

fn event_start_sort_key(event: &AccessibleEvent) -> String {
    format!("{}T{}", event.date, event.time)
}

fn event_start_filetime(event: &AccessibleEvent) -> u64 {
    date_time_to_filetime(&event.date, &event.time)
}

fn event_end_filetime(event: &AccessibleEvent) -> u64 {
    let start = event_start_filetime(event);
    let duration = event.duration_minutes.max(0) as u64 * 60 * 10_000_000;
    start.saturating_add(duration)
}

fn date_time_to_filetime(date: &str, time: &str) -> u64 {
    let year = date
        .get(0..4)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(1970);
    let month = date
        .get(5..7)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let day = date
        .get(8..10)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let hour = time
        .get(0..2)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let minute = time
        .get(3..5)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let days = days_from_civil(year, month, day).max(0) as u64;
    let unix_seconds = days
        .saturating_mul(86_400)
        .saturating_add(u64::from(hour.min(23)) * 3_600)
        .saturating_add(u64::from(minute.min(59)) * 60);
    unix_seconds_to_filetime(unix_seconds)
}

fn filetime_to_date_time(filetime: i64) -> Option<(String, String)> {
    let filetime = u64::try_from(filetime).ok()?;
    let unix_seconds = filetime_to_unix_seconds(filetime)?;
    let days = unix_seconds / 86_400;
    let seconds = unix_seconds % 86_400;
    let (year, month, day) = civil_from_unix_days(days as i64);
    let hour = seconds / 3_600;
    let minute = (seconds % 3_600) / 60;
    Some((
        format!("{year:04}-{month:02}-{day:02}"),
        format!("{hour:02}:{minute:02}"),
    ))
}

fn unix_seconds_to_filetime(unix_seconds: u64) -> u64 {
    unix_seconds
        .saturating_add(11_644_473_600)
        .saturating_mul(10_000_000)
}

fn filetime_to_unix_seconds(filetime: u64) -> Option<u64> {
    filetime
        .checked_div(10_000_000)?
        .checked_sub(11_644_473_600)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
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

fn collaboration_folder_message_class(kind: MapiCollaborationFolderKind) -> &'static str {
    match kind {
        MapiCollaborationFolderKind::Contacts => "IPF.Contact",
        MapiCollaborationFolderKind::Calendar => "IPF.Appointment",
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
        "contacts" => CONTACTS_FOLDER_ID,
        "calendar" => CALENDAR_FOLDER_ID,
        _ => mapi_store_id(uuid_global_counter(&mailbox.id)),
    }
}

fn mapi_message_id(email: &JmapEmail) -> u64 {
    mapi_item_id(&email.id)
}

fn mapi_item_id(id: &Uuid) -> u64 {
    mapi_store_id(uuid_global_counter(id))
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

fn read_u16_prefixed_string(bytes: &[u8], offset: usize) -> Option<String> {
    let size_bytes = bytes.get(offset..offset + 2)?;
    let size = u16::from_le_bytes(size_bytes.try_into().ok()?) as usize;
    let value = bytes.get(offset + 2..offset + 2 + size)?;
    Some(String::from_utf8_lossy(value).into_owned())
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
        if matches!(self.rop_id, 0x0C | 0x11 | 0x25 | 0x70 | 0x72 | 0x7E | 0x82) {
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
        let bytes = if self.rop_id == 0x24 {
            self.payload.get(..4)?
        } else {
            self.payload.get(1..5)?
        };
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

    fn stream_write_data(&self) -> &[u8] {
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(0);
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    fn read_flags(&self) -> Option<u8> {
        match self.rop_id {
            0x11 => self.payload.first().copied(),
            0x66 => self.payload.get(1).copied(),
            _ => None,
        }
    }

    fn sync_type(&self) -> u8 {
        self.payload.first().copied().unwrap_or(0)
    }

    fn fast_transfer_buffer_size(&self) -> usize {
        self.payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(u16::MAX as usize)
    }

    fn stream_data(&self) -> &[u8] {
        let Some(size_bytes) = self.payload.get(..4) else {
            return &[];
        };
        let size = u32::from_le_bytes([size_bytes[0], size_bytes[1], size_bytes[2], size_bytes[3]])
            as usize;
        self.payload.get(4..4 + size).unwrap_or_default()
    }

    fn import_message_id(&self) -> Option<u64> {
        let bytes = self.payload.get(..8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    fn import_property_values(&self) -> Result<Vec<(u32, MapiValue)>> {
        let property_payload = self
            .payload
            .get(8..)
            .ok_or_else(|| anyhow!("missing import property payload"))?;
        RopRequest {
            rop_id: 0x0A,
            input_handle_index: self.input_handle_index,
            output_handle_index: self.output_handle_index,
            payload: property_payload.to_vec(),
        }
        .property_values()
    }

    fn import_hierarchy_values(&self) -> Result<(Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>)> {
        let mut cursor = Cursor::new(self.payload.as_slice());
        let hierarchy_count = cursor.read_u16()? as usize;
        let mut hierarchy_values = Vec::with_capacity(hierarchy_count);
        for _ in 0..hierarchy_count {
            hierarchy_values.push(parse_tagged_property(&mut cursor)?);
        }
        let property_count = cursor.read_u16()? as usize;
        let mut property_values = Vec::with_capacity(property_count);
        for _ in 0..property_count {
            property_values.push(parse_tagged_property(&mut cursor)?);
        }
        Ok((hierarchy_values, property_values))
    }

    fn import_delete_message_ids(&self) -> Vec<u64> {
        let count = self
            .payload
            .get(1..3)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(3..)
            .unwrap_or_default()
            .chunks_exact(8)
            .take(count)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or_default()))
            .collect()
    }

    fn import_move(&self) -> Option<(u64, u64)> {
        let message_id = u64::from_le_bytes(self.payload.get(..8)?.try_into().ok()?);
        let target_folder_id = u64::from_le_bytes(self.payload.get(8..16)?.try_into().ok()?);
        Some((message_id, target_folder_id))
    }

    fn import_read_state_changes(&self) -> Vec<(u64, bool)> {
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(2..)
            .unwrap_or_default()
            .chunks_exact(9)
            .take(count)
            .map(|bytes| {
                let message_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap_or_default());
                let unread = bytes[8] == 0;
                (message_id, unread)
            })
            .collect()
    }

    fn local_replica_midset_deleted(&self) -> &[u8] {
        self.payload.as_slice()
    }

    fn local_replica_id_count(&self) -> u32 {
        self.payload
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(1)
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

    fn status_message_id(&self) -> Option<u64> {
        let bytes = self.payload.get(..8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    fn message_status_flags(&self) -> u32 {
        self.payload
            .get(8..12)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    fn message_status_mask(&self) -> u32 {
        self.payload
            .get(12..16)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    fn create_folder_type(&self) -> u8 {
        self.payload.first().copied().unwrap_or(0)
    }

    fn create_folder_open_existing(&self) -> bool {
        self.payload
            .get(2)
            .is_some_and(|open_existing| *open_existing != 0)
    }

    fn create_folder_display_name(&self) -> String {
        read_u16_prefixed_string(&self.payload, 3).unwrap_or_default()
    }

    fn delete_folder_id(&self) -> Option<u64> {
        let bytes = self.payload.get(1..9)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    fn move_copy_message_ids(&self) -> Vec<u64> {
        let Some(count_bytes) = self.payload.get(..2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload[2..]
            .chunks_exact(8)
            .take(count)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap_or_default()))
            .collect()
    }

    fn move_copy_want_copy(&self) -> bool {
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(2 + count * 8 + 1)
            .is_some_and(|want_copy| *want_copy != 0)
    }

    fn move_copy_target_handle(&self, input_handles: &[u32]) -> Option<u32> {
        input_handles
            .get(self.output_handle_index? as usize)
            .copied()
            .filter(|handle| *handle != u32::MAX)
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
            0x0B | 0x7A => 2,
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

    fn property_ids(&self) -> Vec<u16> {
        let Some(count_bytes) = self.payload.get(..2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(2..)
            .unwrap_or_default()
            .chunks_exact(2)
            .take(count)
            .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
            .collect()
    }

    fn named_property_create(&self) -> bool {
        self.payload.first().is_some_and(|flags| *flags == 0x02)
    }

    fn named_property_names(&self) -> Result<Vec<MapiNamedProperty>> {
        let Some(count_bytes) = self.payload.get(1..3) else {
            return Ok(Vec::new());
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        let mut cursor = Cursor::new(
            self.payload
                .get(3..)
                .ok_or_else(|| anyhow!("missing named property payload"))?,
        );
        let mut properties = Vec::with_capacity(count);
        for _ in 0..count {
            properties.push(parse_named_property(&mut cursor)?);
        }
        Ok(properties)
    }

    fn named_property_query_guid(&self) -> Option<[u8; 16]> {
        if self.payload.get(1).copied().unwrap_or_default() == 0 {
            return None;
        }
        self.payload.get(2..18)?.try_into().ok()
    }

    fn property_values(&self) -> Result<Vec<(u32, MapiValue)>> {
        let Some(size_bytes) = self.payload.get(..2) else {
            return Ok(Vec::new());
        };
        let property_value_size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
        if property_value_size < 2 {
            return Err(anyhow!("invalid property value size"));
        }
        let Some(count_bytes) = self.payload.get(2..4) else {
            return Err(anyhow!("missing property value count"));
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        let value_bytes = self
            .payload
            .get(4..4 + property_value_size - 2)
            .ok_or_else(|| anyhow!("truncated property values"))?;
        let mut cursor = Cursor::new(value_bytes);
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(parse_tagged_property(&mut cursor)?);
        }
        Ok(values)
    }

    fn modify_recipients(&self) -> Result<Vec<PendingRecipientChange>> {
        let Some(count_bytes) = self.payload.get(..2) else {
            return Ok(Vec::new());
        };
        let column_count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        let columns_end = 2 + column_count * 4;
        let columns = self
            .payload
            .get(2..columns_end)
            .ok_or_else(|| anyhow!("truncated recipient columns"))?
            .chunks_exact(4)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect::<Vec<_>>();
        let row_count_bytes = self
            .payload
            .get(columns_end..columns_end + 2)
            .ok_or_else(|| anyhow!("missing recipient row count"))?;
        let row_count = u16::from_le_bytes([row_count_bytes[0], row_count_bytes[1]]) as usize;
        let mut cursor = Cursor::new(
            self.payload
                .get(columns_end + 2..)
                .ok_or_else(|| anyhow!("missing recipient rows"))?,
        );
        let mut changes = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let row_id = cursor.read_u32()?;
            let recipient_type = cursor.read_u8()?;
            let row_size = cursor.read_u16()? as usize;
            if row_size == 0 {
                changes.push(PendingRecipientChange::Delete(row_id));
                continue;
            }
            let row = cursor.read_bytes(row_size)?;
            changes.push(PendingRecipientChange::Upsert(parse_pending_recipient_row(
                row_id,
                recipient_type,
                &columns,
                row,
            )?));
        }
        Ok(changes)
    }
}

fn parse_pending_recipient_row(
    row_id: u32,
    fallback_recipient_type: u8,
    columns: &[u32],
    row: &[u8],
) -> Result<PendingRecipient> {
    let mut cursor = Cursor::new(row);
    let mut values = HashMap::new();
    for column in columns {
        values.insert(*column, parse_property_value_for_tag(&mut cursor, *column)?);
    }
    let recipient_type = values
        .get(&PID_TAG_RECIPIENT_TYPE)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or(fallback_recipient_type);
    let address =
        optional_mapi_value_text(&values, &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W])
            .ok_or_else(|| anyhow!("recipient address is required"))?;
    let display_name = optional_mapi_value_text(&values, &[PID_TAG_DISPLAY_NAME_W])
        .filter(|value| !value.eq_ignore_ascii_case(&address));

    Ok(PendingRecipient {
        row_id,
        recipient_type,
        address,
        display_name,
    })
}

fn optional_mapi_value_text(values: &HashMap<u32, MapiValue>, tags: &[u32]) -> Option<String> {
    tags.iter()
        .find_map(|tag| values.get(tag).and_then(|value| value.clone().into_text()))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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
    parse_tagged_property(cursor).map(|(_property_tag, value)| value)
}

fn parse_tagged_property(cursor: &mut Cursor<'_>) -> Result<(u32, MapiValue)> {
    let property_tag = cursor.read_u32()?;
    let value = parse_property_value_for_tag(cursor, property_tag)?;
    Ok((property_tag, value))
}

fn parse_named_property(cursor: &mut Cursor<'_>) -> Result<MapiNamedProperty> {
    let kind = cursor.read_u8()?;
    let guid: [u8; 16] = cursor
        .read_bytes(16)?
        .try_into()
        .map_err(|_| anyhow!("invalid named property GUID"))?;
    let kind = match kind {
        0x00 => MapiNamedPropertyKind::Lid(cursor.read_u32()?),
        0x01 => {
            let name_size = cursor.read_u8()? as usize;
            let name_bytes = cursor.read_bytes(name_size)?;
            MapiNamedPropertyKind::Name(decode_utf16z_bytes(name_bytes))
        }
        _ => return Err(anyhow!("unsupported named property kind")),
    };
    Ok(MapiNamedProperty { guid, kind })
}

fn decode_utf16z_bytes(bytes: &[u8]) -> String {
    String::from_utf16_lossy(
        &bytes
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .take_while(|unit| *unit != 0)
            .collect::<Vec<_>>(),
    )
}

fn parse_property_value_for_tag(cursor: &mut Cursor<'_>, property_tag: u32) -> Result<MapiValue> {
    let value = match property_tag & 0xFFFF {
        0x0003 => MapiValue::I32(cursor.read_i32()?),
        0x000B => MapiValue::Bool(cursor.read_u8()? != 0),
        0x0014 => MapiValue::I64(cursor.read_i64()?),
        0x001E => MapiValue::String(cursor.read_ascii_z()?),
        0x001F => MapiValue::String(cursor.read_utf16z()?),
        0x0040 => MapiValue::I64(cursor.read_i64()?),
        0x0102 => {
            let len = cursor.read_u16()? as usize;
            MapiValue::Binary(cursor.read_bytes(len)?.to_vec())
        }
        _ => return Err(anyhow!("unsupported MAPI tagged value type")),
    };
    Ok(value)
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
        0x06 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let _code_page_id = cursor.read_u16()?;
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
        0x23 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload: Vec::new(),
            })
        }
        0x24 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x25 => {
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
        0x2D | 0x90 => {
            let input_handle_index = cursor.read_u8()?;
            let size = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(size as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x5D => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
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
        0x70 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let sync_type = cursor.read_u8()?;
            let send_options = cursor.read_u8()?;
            let sync_flags = cursor.read_u16()?;
            let mut payload = vec![sync_type, send_options];
            payload.extend_from_slice(&sync_flags.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x4E => {
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
        0x75 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x76 => {
            let input_handle_index = cursor.read_u8()?;
            let stream_size = cursor.read_u32()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(stream_size as u32).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(stream_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x77 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x7E | 0x82 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload: Vec::new(),
            })
        }
        0x72 => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            let property_value_size = cursor.read_u16()? as usize;
            let property_value_count = cursor.read_u16()?;
            payload.extend_from_slice(&(property_value_size as u16).to_le_bytes());
            payload.extend_from_slice(&property_value_count.to_le_bytes());
            let values_size = property_value_size
                .checked_sub(2)
                .ok_or_else(|| anyhow!("invalid import property value size"))?;
            payload.extend_from_slice(cursor.read_bytes(values_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x73 => {
            let input_handle_index = cursor.read_u8()?;
            let start = cursor.position;
            let hierarchy_count = cursor.read_u16()? as usize;
            for _ in 0..hierarchy_count {
                parse_tagged_property(cursor)?;
            }
            let property_count = cursor.read_u16()? as usize;
            for _ in 0..property_count {
                parse_tagged_property(cursor)?;
            }
            let end = cursor.position;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.bytes[start..end].to_vec(),
            })
        }
        0x74 => {
            let input_handle_index = cursor.read_u8()?;
            let delete_flags = cursor.read_u8()?;
            let message_id_count = cursor.read_u16()? as usize;
            let mut payload = vec![delete_flags];
            payload.extend_from_slice(&(message_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(message_id_count * 8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x78 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x80 => {
            let input_handle_index = cursor.read_u8()?;
            let change_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(change_count as u16).to_le_bytes());
            for _ in 0..change_count {
                payload.extend_from_slice(cursor.read_bytes(8)?);
                payload.push(cursor.read_u8()?);
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x93 => {
            let input_handle_index = cursor.read_u8()?;
            let size = cursor.read_u16()? as usize;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: cursor.read_bytes(size)?.to_vec(),
            })
        }
        0x7F => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x09 | 0x16 | 0x17 | 0x52 | 0x68 | 0x7B | 0x81 => {
            let input_handle_index = cursor.read_u8()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x10 => {
            let input_handle_index = cursor.read_u8()?;
            let _reserved = cursor.read_u16()?;
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
        0x1F => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x20 => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(cursor.read_bytes(8)?);
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
            payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
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
        0x1C => {
            let input_handle_index = cursor.read_u8()?;
            let output_handle_index = cursor.read_u8()?;
            let folder_type = cursor.read_u8()?;
            let use_unicode = cursor.read_u8()? != 0;
            let open_existing = cursor.read_u8()?;
            let _reserved = cursor.read_u8()?;
            let display_name = if use_unicode {
                cursor.read_utf16z()?
            } else {
                cursor.read_ascii_z()?
            };
            let comment = if use_unicode {
                cursor.read_utf16z()?
            } else {
                cursor.read_ascii_z()?
            };
            let mut payload = vec![folder_type, use_unicode as u8, open_existing];
            write_u16_prefixed_bytes(&mut payload, display_name.as_bytes());
            write_u16_prefixed_bytes(&mut payload, comment.as_bytes());
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: Some(output_handle_index),
                payload,
            })
        }
        0x1D => {
            let input_handle_index = cursor.read_u8()?;
            let mut payload = vec![cursor.read_u8()?];
            payload.extend_from_slice(cursor.read_bytes(8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x1E | 0x91 => {
            let input_handle_index = cursor.read_u8()?;
            let want_asynchronous = cursor.read_u8()?;
            let notify_non_read = cursor.read_u8()?;
            let message_id_count = cursor.read_u16()? as usize;
            let mut payload = vec![want_asynchronous, notify_non_read];
            payload.extend_from_slice(&(message_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(message_id_count * 8)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x0A | 0x79 => {
            let input_handle_index = cursor.read_u8()?;
            let property_value_size = cursor.read_u16()? as usize;
            let property_value_count = cursor.read_u16()?;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(property_value_size as u16).to_le_bytes());
            payload.extend_from_slice(&property_value_count.to_le_bytes());
            let values_size = property_value_size
                .checked_sub(2)
                .ok_or_else(|| anyhow!("invalid RopSetProperties value size"))?;
            payload.extend_from_slice(cursor.read_bytes(values_size)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x0B | 0x7A => {
            let input_handle_index = cursor.read_u8()?;
            let property_tag_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(property_tag_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_tag_count * 4)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
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
        0x0C => {
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
        0x0D => {
            let input_handle_index = cursor.read_u8()?;
            let _reserved = cursor.read_u32()?;
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload: Vec::new(),
            })
        }
        0x0E => {
            let input_handle_index = cursor.read_u8()?;
            let column_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(column_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(column_count * 4)?);
            let row_count = cursor.read_u16()? as usize;
            payload.extend_from_slice(&(row_count as u16).to_le_bytes());
            for _ in 0..row_count {
                payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes());
                payload.push(cursor.read_u8()?);
                let row_size = cursor.read_u16()? as usize;
                payload.extend_from_slice(&(row_size as u16).to_le_bytes());
                payload.extend_from_slice(cursor.read_bytes(row_size)?);
            }
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
        0x32 => {
            let input_handle_index = cursor.read_u8()?;
            let payload = vec![cursor.read_u8()?];
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x33 => {
            let source_handle_index = cursor.read_u8()?;
            let dest_handle_index = cursor.read_u8()?;
            let message_id_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(message_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(message_id_count * 8)?);
            payload.push(cursor.read_u8()?);
            payload.push(cursor.read_u8()?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(source_handle_index),
                output_handle_index: Some(dest_handle_index),
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
        0x55 => {
            let input_handle_index = cursor.read_u8()?;
            let property_id_count = cursor.read_u16()? as usize;
            let mut payload = Vec::new();
            payload.extend_from_slice(&(property_id_count as u16).to_le_bytes());
            payload.extend_from_slice(cursor.read_bytes(property_id_count * 2)?);
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x56 => {
            let input_handle_index = cursor.read_u8()?;
            let flags = cursor.read_u8()?;
            let property_name_count = cursor.read_u16()? as usize;
            let mut payload = vec![flags];
            payload.extend_from_slice(&(property_name_count as u16).to_le_bytes());
            for _ in 0..property_name_count {
                let kind = cursor.read_u8()?;
                payload.push(kind);
                payload.extend_from_slice(cursor.read_bytes(16)?);
                match kind {
                    0x00 => payload.extend_from_slice(&cursor.read_u32()?.to_le_bytes()),
                    0x01 => {
                        let name_size = cursor.read_u8()? as usize;
                        payload.push(name_size as u8);
                        payload.extend_from_slice(cursor.read_bytes(name_size)?);
                    }
                    _ => return Err(anyhow!("unsupported named property kind")),
                }
            }
            Ok(RopRequest {
                rop_id,
                input_handle_index: Some(input_handle_index),
                output_handle_index: None,
                payload,
            })
        }
        0x5F => {
            let input_handle_index = cursor.read_u8()?;
            let query_flags = cursor.read_u8()?;
            let has_guid = cursor.read_u8()?;
            let mut payload = vec![query_flags, has_guid];
            if has_guid != 0 {
                payload.extend_from_slice(cursor.read_bytes(16)?);
            }
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
            message_statuses: HashMap::new(),
            named_properties: HashMap::new(),
            named_property_ids: HashMap::new(),
            next_named_property_id: FIRST_NAMED_PROPERTY_ID,
            next_local_replica_sequence: 1,
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
