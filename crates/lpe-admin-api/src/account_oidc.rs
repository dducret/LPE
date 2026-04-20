use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use lpe_storage::{AccountOidcClaims, SecuritySettings};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

const CALLBACK_PATH: &str = "/api/mail/auth/oidc/callback";
const MAX_STATE_AGE_SECONDS: i64 = 900;

#[derive(Debug, Serialize, Deserialize)]
struct OidcStatePayload {
    issued_at: i64,
    redirect_uri: String,
}

#[derive(Debug, Deserialize)]
struct OidcTokenResponse {
    access_token: String,
    token_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OidcDiscoveryDocument {
    authorization_endpoint: String,
    token_endpoint: String,
    userinfo_endpoint: String,
}

#[derive(Debug, Clone)]
struct OidcResolvedEndpoints {
    authorization_endpoint: String,
    token_endpoint: String,
    userinfo_endpoint: String,
}

pub async fn authorization_url(settings: &SecuritySettings, public_origin: &str) -> Result<String> {
    ensure_oidc_ready(settings)?;
    let redirect_uri = callback_url(public_origin)?;
    let state = sign_state(
        &OidcStatePayload {
            issued_at: now_unix(),
            redirect_uri: redirect_uri.clone(),
        },
        &settings.mailbox_oidc_client_secret,
    )?;
    let endpoints = resolved_endpoints(settings).await?;
    let mut url = Url::parse(&endpoints.authorization_endpoint)
        .context("invalid mailbox OIDC authorization endpoint")?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", settings.mailbox_oidc_client_id.trim())
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("scope", normalized_scopes(settings))
        .append_pair("state", &state);
    Ok(url.to_string())
}

pub async fn exchange_code_for_claims(
    settings: &SecuritySettings,
    public_origin: &str,
    code: &str,
    state: &str,
) -> Result<AccountOidcClaims> {
    ensure_oidc_ready(settings)?;
    let redirect_uri = callback_url(public_origin)?;
    verify_state(state, &settings.mailbox_oidc_client_secret, &redirect_uri)?;
    let endpoints = resolved_endpoints(settings).await?;

    let token = Client::new()
        .post(&endpoints.token_endpoint)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code.trim()),
            ("redirect_uri", redirect_uri.as_str()),
            ("client_id", settings.mailbox_oidc_client_id.trim()),
            ("client_secret", settings.mailbox_oidc_client_secret.trim()),
        ])
        .send()
        .await
        .context("mailbox OIDC token exchange failed")?
        .error_for_status()
        .context("mailbox OIDC token endpoint returned an error")?
        .json::<OidcTokenResponse>()
        .await
        .context("mailbox OIDC token response is invalid")?;

    if token.access_token.trim().is_empty() {
        bail!("mailbox OIDC token endpoint returned an empty access token");
    }

    if let Some(token_type) = token.token_type.as_deref() {
        if !token_type.eq_ignore_ascii_case("bearer") {
            bail!("mailbox OIDC token endpoint returned an unsupported token type");
        }
    }

    let userinfo = Client::new()
        .get(&endpoints.userinfo_endpoint)
        .bearer_auth(token.access_token)
        .send()
        .await
        .context("mailbox OIDC userinfo request failed")?
        .error_for_status()
        .context("mailbox OIDC userinfo endpoint returned an error")?
        .json::<Value>()
        .await
        .context("mailbox OIDC userinfo payload is invalid")?;

    let issuer_url = settings.mailbox_oidc_issuer_url.trim().to_string();
    let subject =
        claim_string(&userinfo, &settings.mailbox_oidc_claim_subject).ok_or_else(|| {
            anyhow!("mailbox OIDC userinfo does not contain the configured subject claim")
        })?;
    let email = claim_string(&userinfo, &settings.mailbox_oidc_claim_email).ok_or_else(|| {
        anyhow!("mailbox OIDC userinfo does not contain the configured email claim")
    })?;
    let display_name = claim_string(&userinfo, &settings.mailbox_oidc_claim_display_name)
        .unwrap_or_else(|| email.clone());

    Ok(AccountOidcClaims {
        issuer_url,
        subject,
        email: email.trim().to_lowercase(),
        display_name,
    })
}

fn ensure_oidc_ready(settings: &SecuritySettings) -> Result<()> {
    if !settings.mailbox_oidc_login_enabled {
        bail!("mailbox OIDC login is disabled");
    }

    for (name, value) in [
        ("issuer", settings.mailbox_oidc_issuer_url.trim()),
        ("client id", settings.mailbox_oidc_client_id.trim()),
        ("client secret", settings.mailbox_oidc_client_secret.trim()),
    ] {
        if value.is_empty() {
            bail!("mailbox OIDC {name} is required");
        }
    }

    Ok(())
}

fn callback_url(public_origin: &str) -> Result<String> {
    let base = public_origin.trim().trim_end_matches('/');
    if base.is_empty() {
        bail!("public origin is required");
    }
    Ok(format!("{base}{CALLBACK_PATH}"))
}

fn normalized_scopes(settings: &SecuritySettings) -> &str {
    let scopes = settings.mailbox_oidc_scopes.trim();
    if scopes.is_empty() {
        "openid profile email"
    } else {
        scopes
    }
}

async fn resolved_endpoints(settings: &SecuritySettings) -> Result<OidcResolvedEndpoints> {
    if !settings
        .mailbox_oidc_authorization_endpoint
        .trim()
        .is_empty()
        && !settings.mailbox_oidc_token_endpoint.trim().is_empty()
        && !settings.mailbox_oidc_userinfo_endpoint.trim().is_empty()
    {
        return Ok(OidcResolvedEndpoints {
            authorization_endpoint: settings
                .mailbox_oidc_authorization_endpoint
                .trim()
                .to_string(),
            token_endpoint: settings.mailbox_oidc_token_endpoint.trim().to_string(),
            userinfo_endpoint: settings.mailbox_oidc_userinfo_endpoint.trim().to_string(),
        });
    }

    let issuer = settings
        .mailbox_oidc_issuer_url
        .trim()
        .trim_end_matches('/');
    let discovery_url = format!("{issuer}/.well-known/openid-configuration");
    let document = Client::new()
        .get(&discovery_url)
        .send()
        .await
        .context("mailbox OIDC discovery request failed")?
        .error_for_status()
        .context("mailbox OIDC discovery endpoint returned an error")?
        .json::<OidcDiscoveryDocument>()
        .await
        .context("mailbox OIDC discovery payload is invalid")?;

    Ok(OidcResolvedEndpoints {
        authorization_endpoint: document.authorization_endpoint,
        token_endpoint: document.token_endpoint,
        userinfo_endpoint: document.userinfo_endpoint,
    })
}

fn sign_state(payload: &OidcStatePayload, secret: &str) -> Result<String> {
    let payload_json = serde_json::to_vec(payload)?;
    let payload_encoded = URL_SAFE_NO_PAD.encode(payload_json);
    let signature = state_signature(&payload_encoded, secret);
    Ok(format!("{payload_encoded}.{signature}"))
}

fn verify_state(state: &str, secret: &str, expected_redirect_uri: &str) -> Result<()> {
    let (payload_encoded, signature) = state
        .split_once('.')
        .ok_or_else(|| anyhow!("mailbox OIDC state is malformed"))?;
    if state_signature(payload_encoded, secret) != signature {
        bail!("mailbox OIDC state signature is invalid");
    }

    let payload = URL_SAFE_NO_PAD
        .decode(payload_encoded)
        .context("mailbox OIDC state payload is invalid")?;
    let payload: OidcStatePayload =
        serde_json::from_slice(&payload).context("mailbox OIDC state payload is malformed")?;
    if payload.redirect_uri != expected_redirect_uri {
        bail!("mailbox OIDC state redirect target is invalid");
    }
    if now_unix() - payload.issued_at > MAX_STATE_AGE_SECONDS {
        bail!("mailbox OIDC state has expired");
    }
    Ok(())
}

fn state_signature(payload_encoded: &str, secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.update(b".");
    hasher.update(payload_encoded.as_bytes());
    hasher.update(b".");
    hasher.update(secret.as_bytes());
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}

fn claim_string(value: &Value, path: &str) -> Option<String> {
    let mut current = value;
    for segment in path.trim().split('.') {
        if segment.is_empty() {
            return None;
        }
        current = current.get(segment)?;
    }
    current.as_str().map(ToString::to_string)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
