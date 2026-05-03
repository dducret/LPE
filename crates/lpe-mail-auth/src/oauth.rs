use anyhow::{anyhow, bail, Result};
use axum::http::HeaderMap;
use base64::{
    engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD},
    Engine as _,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::auth::normalize_login_name;

type HmacSha256 = Hmac<Sha256>;

const MAIL_OAUTH_SIGNING_SECRET_ENV: &str = "LPE_MAIL_OAUTH_SIGNING_SECRET";
const MIN_OAUTH_SIGNING_SECRET_LEN: usize = 32;
pub const DEFAULT_OAUTH_ACCESS_SCOPE: &str = "mail imap dav activesync ews managesieve smtp";
pub const DEFAULT_OAUTH_ACCESS_TOKEN_SECONDS: u32 = 3600;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountPrincipal {
    pub tenant_id: String,
    pub account_id: uuid::Uuid,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OAuthAccessTokenClaims {
    kind: String,
    version: u8,
    tenant_id: String,
    account_id: uuid::Uuid,
    email: String,
    scope: String,
    exp: u64,
}

pub fn issue_oauth_access_token(
    principal: &AccountPrincipal,
    scope: &str,
    expires_in_seconds: u32,
) -> Result<String> {
    let scope = normalize_scope(scope)?;
    let secret = oauth_signing_secret()?;
    let claims = OAuthAccessTokenClaims {
        kind: "lpe-mail-oauth-access".to_string(),
        version: 1,
        tenant_id: principal.tenant_id.clone(),
        account_id: principal.account_id,
        email: principal.email.trim().to_lowercase(),
        scope,
        exp: unix_time().saturating_add(expires_in_seconds.max(60) as u64),
    };
    encode_oauth_access_token(&claims, &secret)
}

pub fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub fn basic_credentials(headers: &HeaderMap) -> Result<Option<(String, String)>> {
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

pub fn normalize_scope(scope: &str) -> Result<String> {
    let mut values = scope
        .split_whitespace()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.is_empty() {
        bail!("oauth access token scope is required");
    }
    values.sort();
    values.dedup();
    for value in &values {
        if !matches!(
            value.as_str(),
            "mail" | "imap" | "dav" | "activesync" | "ews" | "managesieve" | "smtp"
        ) {
            bail!("unsupported oauth access token scope {value}");
        }
    }
    Ok(values.join(" "))
}

pub fn oauth_signing_secret() -> Result<String> {
    let value = env::var(MAIL_OAUTH_SIGNING_SECRET_ENV)
        .map_err(|_| anyhow!("{MAIL_OAUTH_SIGNING_SECRET_ENV} must be set"))?;
    let value = value.trim().to_string();
    if value.is_empty() {
        bail!("{MAIL_OAUTH_SIGNING_SECRET_ENV} must not be empty");
    }
    if value.len() < MIN_OAUTH_SIGNING_SECRET_LEN {
        bail!(
            "{MAIL_OAUTH_SIGNING_SECRET_ENV} must contain at least {MIN_OAUTH_SIGNING_SECRET_LEN} characters"
        );
    }
    if is_known_weak_secret(&value) {
        bail!("{MAIL_OAUTH_SIGNING_SECRET_ENV} uses a forbidden weak placeholder value");
    }
    Ok(value)
}

pub(crate) fn decode_oauth_access_token(token: &str) -> Result<AccountPrincipalClaims> {
    let (encoded_payload, encoded_signature) = token
        .trim()
        .split_once('.')
        .ok_or_else(|| anyhow!("oauth access token is malformed"))?;
    let payload = URL_SAFE_NO_PAD
        .decode(encoded_payload)
        .map_err(|_| anyhow!("oauth access token payload is invalid"))?;
    let signature = URL_SAFE_NO_PAD
        .decode(encoded_signature)
        .map_err(|_| anyhow!("oauth access token signature is invalid"))?;
    let secret = oauth_signing_secret()?;
    verify_signature(&secret, &payload, &signature)?;

    let mut claims: OAuthAccessTokenClaims = serde_json::from_slice(&payload)
        .map_err(|_| anyhow!("oauth access token claims are invalid"))?;
    if claims.kind != "lpe-mail-oauth-access" || claims.version != 1 {
        bail!("oauth access token uses an unsupported format");
    }
    if claims.exp <= unix_time() {
        bail!("oauth access token has expired");
    }
    claims.scope = normalize_scope(&claims.scope)?;
    Ok(AccountPrincipalClaims {
        tenant_id: claims.tenant_id,
        account_id: claims.account_id,
        email: normalize_login_name(&claims.email, None),
        scope: claims.scope,
    })
}

pub(crate) struct AccountPrincipalClaims {
    pub(crate) tenant_id: String,
    pub(crate) account_id: uuid::Uuid,
    pub(crate) email: String,
    pub(crate) scope: String,
}

fn encode_oauth_access_token(claims: &OAuthAccessTokenClaims, secret: &str) -> Result<String> {
    let payload = serde_json::to_vec(claims)?;
    let signature = sign_payload(secret, &payload)?;
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(payload),
        URL_SAFE_NO_PAD.encode(signature)
    ))
}

fn sign_payload(secret: &str, payload: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("oauth signing secret is invalid"))?;
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify_signature(secret: &str, payload: &[u8], signature: &[u8]) -> Result<()> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("oauth signing secret is invalid"))?;
    mac.update(payload);
    mac.verify_slice(signature)
        .map_err(|_| anyhow!("oauth access token signature is invalid"))?;
    Ok(())
}

pub(crate) fn scope_allows_surface(scope: &str, surface: &str) -> bool {
    let expected = surface.trim().to_ascii_lowercase();
    scope
        .split_whitespace()
        .any(|entry| entry.eq_ignore_ascii_case(&expected))
}

pub fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn is_known_weak_secret(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "change-me"
            | "changeme"
            | "secret"
            | "password"
            | "default"
            | "test"
            | "example"
            | "oauth-secret"
            | "mail-oauth-secret"
    )
}
