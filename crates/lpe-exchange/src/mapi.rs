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
            MapiRequestType::Bind => "Bind",
            MapiRequestType::Unbind => "Unbind",
            MapiRequestType::Ping => "PING",
            MapiRequestType::Unsupported(value) => value,
        }
    }
}
