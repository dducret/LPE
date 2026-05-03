use anyhow::{anyhow, Result};
use axum::{
    http::{
        header::{CONTENT_TYPE, SET_COOKIE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};
use uuid::Uuid;

use crate::store::ExchangeStore;

const MAPI_CONTENT_TYPE: &str = "application/mapi-http";
const MAPI_SERVER_APPLICATION: &str = "LPE/0.1.3";
const EMSMDB_COOKIE: &str = "lpe_mapi_emsmdb";
const NSPI_COOKIE: &str = "lpe_mapi_nspi";
const EMSMDB_COOKIE_PATH: &str = "/mapi/emsmdb";
const NSPI_COOKIE_PATH: &str = "/mapi/nspi";

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
    Ping,
    Unsupported(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MapiSession {
    endpoint: MapiEndpoint,
    tenant_id: String,
    account_id: Uuid,
    email: String,
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

    let response = match (endpoint, request_type) {
        (MapiEndpoint::Emsmdb, MapiRequestType::Connect) => {
            connect_response(endpoint, &principal, &request_id)
        }
        (MapiEndpoint::Emsmdb, MapiRequestType::Disconnect) => {
            disconnect_response(endpoint, &principal, headers, &request_id, "Disconnect")
        }
        (MapiEndpoint::Emsmdb, MapiRequestType::Execute) => {
            execute_response(endpoint, &principal, headers, _body, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::Bind) => {
            bind_response(endpoint, &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::Unbind) => {
            disconnect_response(endpoint, &principal, headers, &request_id, "Unbind")
        }
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
    body.extend_from_slice(Uuid::nil().as_bytes());
    write_u32(&mut body, 0);
    mapi_response("Bind", request_id, 0, body, Some(cookie))
}

fn execute_response(
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
    let rop_buffer = execute_rops(principal, &execute.rop_buffer);
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

fn create_session(endpoint: MapiEndpoint, principal: &AccountPrincipal) -> String {
    let session_id = Uuid::new_v4().to_string();
    let session = MapiSession {
        endpoint,
        tenant_id: principal.tenant_id.clone(),
        account_id: principal.account_id,
        email: principal.email.clone(),
    };
    sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(session_id.clone(), session);
    session_id
}

fn remove_session(session_id: &str) -> Option<MapiSession> {
    sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(session_id)
}

fn get_session(session_id: &str) -> Option<MapiSession> {
    sessions()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(session_id)
        .cloned()
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
        .unwrap_or("00000000-0000-0000-0000-000000000000")
        .to_string()
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
    let mut response = (StatusCode::OK, body).into_response();
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

fn execute_rops(principal: &AccountPrincipal, rop_buffer: &[u8]) -> Vec<u8> {
    let Some((requests, _handle_table)) = split_rop_buffer(rop_buffer) else {
        return rop_buffer_with_response(unsupported_rop_response(0, 0), None);
    };
    if requests.is_empty() {
        return rop_buffer_with_response(Vec::new(), None);
    }

    match requests[0] {
        0x01 => rop_buffer_with_response(Vec::new(), None),
        0xFE => rop_logon_response(principal, requests),
        rop_id => {
            let handle_index = requests.get(2).copied().unwrap_or(0);
            rop_buffer_with_response(unsupported_rop_response(rop_id, handle_index), None)
        }
    }
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

fn rop_logon_response(principal: &AccountPrincipal, request: &[u8]) -> Vec<u8> {
    if request.len() < 14 {
        return rop_buffer_with_response(rop_logon_failure_response(0, 0x8004_0102), None);
    }
    let output_handle_index = request[2];
    let logon_flags = request[3] | 0x01;
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
    rop_buffer_with_response(response, Some(1))
}

fn rop_logon_failure_response(output_handle_index: u8, return_value: u32) -> Vec<u8> {
    let mut response = vec![0xFE, output_handle_index];
    write_u32(&mut response, return_value);
    response
}

fn unsupported_rop_response(rop_id: u8, handle_index: u8) -> Vec<u8> {
    let mut response = vec![rop_id, handle_index];
    write_u32(&mut response, 0x8004_0102);
    response
}

fn rop_buffer_with_response(response: Vec<u8>, output_handle: Option<u32>) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&(response.len() as u16).to_le_bytes());
    buffer.extend_from_slice(&response);
    if let Some(handle) = output_handle {
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
        format!("{name}={session_id}; Path={path}; HttpOnly; SameSite=Lax; Secure")
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

fn write_u64(body: &mut Vec<u8>, value: u64) {
    body.extend_from_slice(&value.to_le_bytes());
}

fn write_utf16z(body: &mut Vec<u8>, value: &str) {
    for unit in value.encode_utf16() {
        body.extend_from_slice(&unit.to_le_bytes());
    }
    body.extend_from_slice(&0u16.to_le_bytes());
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
}
