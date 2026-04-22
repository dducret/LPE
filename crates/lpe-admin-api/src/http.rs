use axum::http::{HeaderMap, StatusCode};
use std::env;

pub(crate) fn internal_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

pub(crate) fn bad_request_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, error.to_string())
}

pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(ToString::to_string)
}

pub(crate) fn public_origin(headers: &HeaderMap) -> String {
    let scheme = forwarded_header(headers, "x-forwarded-proto")
        .or_else(|| env::var("LPE_PUBLIC_SCHEME").ok())
        .unwrap_or_else(|| "http".to_string());
    let host = forwarded_header(headers, "x-forwarded-host")
        .or_else(|| forwarded_header(headers, "host"))
        .or_else(|| env::var("LPE_PUBLIC_HOSTNAME").ok())
        .unwrap_or_else(|| "localhost".to_string());
    format!("{}://{}", scheme.trim(), host.trim())
}

pub(crate) fn forwarded_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
