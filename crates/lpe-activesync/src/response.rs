use anyhow::Result;
use axum::{
    http::{
        header::{CONTENT_TYPE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::Response,
};
use uuid::Uuid;

use crate::{
    constants::{ACTIVE_SYNC_COMMANDS, ACTIVE_SYNC_VERSION},
    wbxml::WbxmlNode,
};

pub(crate) fn empty_response() -> Response {
    let mut response = Response::new(axum::body::Body::empty());
    *response.status_mut() = StatusCode::OK;
    add_common_headers(response.headers_mut());
    response
}

pub(crate) fn auth_challenge_response() -> Response {
    let mut response = Response::new(axum::body::Body::empty());
    *response.status_mut() = StatusCode::UNAUTHORIZED;
    add_common_headers(response.headers_mut());
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"LPE ActiveSync\""),
    );
    response
}

pub(crate) fn wbxml_response(protocol_version: &str, body: Vec<u8>) -> Result<Response> {
    let mut response = Response::new(axum::body::Body::from(body));
    *response.status_mut() = StatusCode::OK;
    add_common_headers(response.headers_mut());
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/vnd.ms-sync.wbxml"),
    );
    response.headers_mut().insert(
        "ms-asprotocolversion",
        HeaderValue::from_str(protocol_version)?,
    );
    Ok(response)
}

fn add_common_headers(headers: &mut HeaderMap) {
    headers.insert("allow", HeaderValue::from_static("OPTIONS, POST"));
    headers.insert(
        "ms-server-activesync",
        HeaderValue::from_static(ACTIVE_SYNC_VERSION),
    );
    headers.insert(
        "ms-asprotocolversions",
        HeaderValue::from_static(ACTIVE_SYNC_VERSION),
    );
    headers.insert(
        "ms-asprotocolcommands",
        HeaderValue::from_static(ACTIVE_SYNC_COMMANDS),
    );
    headers.insert("public", HeaderValue::from_static("OPTIONS, POST"));
    headers.insert("dav", HeaderValue::from_static("1,2"));
}

pub(crate) fn error_response(error: anyhow::Error) -> Response {
    let message = error.to_string();
    if is_authentication_error(&message) {
        return auth_challenge_response();
    }

    let mut response = Response::new(axum::body::Body::from(message));
    *response.status_mut() = StatusCode::BAD_REQUEST;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn is_authentication_error(message: &str) -> bool {
    message == "missing account authentication"
        || message == "invalid credentials"
        || message.starts_with("oauth access token ")
        || message.contains(" credentials")
}

pub(crate) fn sync_status_node(collection_id: &str, status: &str) -> WbxmlNode {
    let mut collection = WbxmlNode::new(0, "Collection");
    collection.push(WbxmlNode::with_text(0, "CollectionId", collection_id));
    collection.push(WbxmlNode::with_text(0, "Status", status));
    collection
}

pub(crate) fn policy_key(account_id: Uuid, device_id: &str) -> String {
    let seed = format!("{account_id}:{device_id}");
    let mut value: u32 = 0;
    for byte in seed.bytes() {
        value = value.wrapping_mul(33).wrapping_add(byte as u32);
    }
    value.max(1).to_string()
}

pub(crate) fn is_message_rfc822(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().starts_with("message/rfc822"))
        .unwrap_or(false)
}
