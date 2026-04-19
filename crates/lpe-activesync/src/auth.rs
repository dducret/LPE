use anyhow::{anyhow, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use axum::http::HeaderMap;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

use crate::constants::ACTIVE_SYNC_VERSION;

pub(crate) fn protocol_version(headers: &HeaderMap) -> String {
    headers
        .get("ms-asprotocolversion")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(ACTIVE_SYNC_VERSION)
        .to_string()
}

pub(crate) fn normalize_login_name(username: &str, hinted_user: Option<&str>) -> String {
    if username.contains('@') {
        username.trim().to_lowercase()
    } else {
        hinted_user.unwrap_or(username).trim().to_lowercase()
    }
}

pub(crate) fn verify_password(password_hash: &str, password: &str) -> bool {
    PasswordHash::new(password_hash)
        .ok()
        .and_then(|parsed| {
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .ok()
        })
        .is_some()
}

pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub(crate) fn basic_credentials(headers: &HeaderMap) -> Result<Option<(String, String)>> {
    let Some(value) = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(None);
    };
    let Some(encoded) = value.strip_prefix("Basic ") else {
        return Ok(None);
    };
    let decoded = BASE64.decode(encoded.trim())?;
    let decoded = String::from_utf8(decoded)?;
    let (username, password) = decoded
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid basic authorization header"))?;
    Ok(Some((username.to_string(), password.to_string())))
}
