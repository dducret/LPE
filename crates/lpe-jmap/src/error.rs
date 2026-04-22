use anyhow::Error;
use axum::http::StatusCode;
use serde_json::{json, Value};

pub(crate) fn http_error(error: Error) -> (StatusCode, String) {
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

pub(crate) fn method_error(kind: &str, description: &str) -> Value {
    json!({
        "type": kind,
        "description": description,
    })
}

pub(crate) fn set_error(description: &str) -> Value {
    method_error("invalidProperties", description)
}
