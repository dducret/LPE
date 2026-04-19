use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use lpe_storage::{AdminOidcClaims, SecuritySettings};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

const CALLBACK_PATH: &str = "/api/auth/oidc/callback";
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

pub fn authorization_url(settings: &SecuritySettings, public_origin: &str) -> Result<String> {
    ensure_oidc_ready(settings)?;
    let redirect_uri = callback_url(public_origin)?;
    let state = sign_state(
        &OidcStatePayload {
            issued_at: now_unix(),
            redirect_uri: redirect_uri.clone(),
        },
        &settings.oidc_client_secret,
    )?;
    let mut url = Url::parse(settings.oidc_authorization_endpoint.trim())
        .context("invalid OIDC authorization endpoint")?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", settings.oidc_client_id.trim())
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
) -> Result<AdminOidcClaims> {
    ensure_oidc_ready(settings)?;
    let redirect_uri = callback_url(public_origin)?;
    verify_state(state, &settings.oidc_client_secret, &redirect_uri)?;

    let token = Client::new()
        .post(settings.oidc_token_endpoint.trim())
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code.trim()),
            ("redirect_uri", redirect_uri.as_str()),
            ("client_id", settings.oidc_client_id.trim()),
            ("client_secret", settings.oidc_client_secret.trim()),
        ])
        .send()
        .await
        .context("OIDC token exchange failed")?
        .error_for_status()
        .context("OIDC token endpoint returned an error")?
        .json::<OidcTokenResponse>()
        .await
        .context("OIDC token response is invalid")?;

    if token.access_token.trim().is_empty() {
        bail!("OIDC token endpoint returned an empty access token");
    }

    if let Some(token_type) = token.token_type.as_deref() {
        if !token_type.eq_ignore_ascii_case("bearer") {
            bail!("OIDC token endpoint returned an unsupported token type");
        }
    }

    let userinfo = Client::new()
        .get(settings.oidc_userinfo_endpoint.trim())
        .bearer_auth(token.access_token)
        .send()
        .await
        .context("OIDC userinfo request failed")?
        .error_for_status()
        .context("OIDC userinfo endpoint returned an error")?
        .json::<Value>()
        .await
        .context("OIDC userinfo payload is invalid")?;

    let issuer_url = settings.oidc_issuer_url.trim().to_string();
    let subject = claim_string(&userinfo, &settings.oidc_claim_subject)
        .ok_or_else(|| anyhow!("OIDC userinfo does not contain the configured subject claim"))?;
    let email = claim_string(&userinfo, &settings.oidc_claim_email)
        .ok_or_else(|| anyhow!("OIDC userinfo does not contain the configured email claim"))?;
    let display_name =
        claim_string(&userinfo, &settings.oidc_claim_display_name).unwrap_or_else(|| email.clone());

    Ok(AdminOidcClaims {
        issuer_url,
        subject,
        email: email.trim().to_lowercase(),
        display_name,
    })
}

fn ensure_oidc_ready(settings: &SecuritySettings) -> Result<()> {
    if !settings.oidc_login_enabled {
        bail!("OIDC login is disabled");
    }

    for (name, value) in [
        ("issuer", settings.oidc_issuer_url.trim()),
        (
            "authorization endpoint",
            settings.oidc_authorization_endpoint.trim(),
        ),
        ("token endpoint", settings.oidc_token_endpoint.trim()),
        ("userinfo endpoint", settings.oidc_userinfo_endpoint.trim()),
        ("client id", settings.oidc_client_id.trim()),
        ("client secret", settings.oidc_client_secret.trim()),
    ] {
        if value.is_empty() {
            bail!("OIDC {name} is required");
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
    let scopes = settings.oidc_scopes.trim();
    if scopes.is_empty() {
        "openid profile email"
    } else {
        scopes
    }
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
        .ok_or_else(|| anyhow!("OIDC state is malformed"))?;
    if state_signature(payload_encoded, secret) != signature {
        bail!("OIDC state signature is invalid");
    }

    let payload = URL_SAFE_NO_PAD
        .decode(payload_encoded)
        .context("OIDC state payload is invalid")?;
    let payload: OidcStatePayload =
        serde_json::from_slice(&payload).context("OIDC state payload is malformed")?;
    if payload.redirect_uri != expected_redirect_uri {
        bail!("OIDC state redirect target is invalid");
    }
    if now_unix() - payload.issued_at > MAX_STATE_AGE_SECONDS {
        bail!("OIDC state has expired");
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

#[cfg(test)]
mod tests {
    use super::{authorization_url, verify_state, SecuritySettings};

    fn settings() -> SecuritySettings {
        SecuritySettings {
            password_login_enabled: true,
            mfa_required_for_admins: false,
            session_timeout_minutes: 45,
            audit_retention_days: 365,
            oidc_login_enabled: true,
            oidc_provider_label: "Corporate SSO".to_string(),
            oidc_auto_link_by_email: true,
            oidc_issuer_url: "https://issuer.example.test".to_string(),
            oidc_authorization_endpoint: "https://issuer.example.test/authorize".to_string(),
            oidc_token_endpoint: "https://issuer.example.test/token".to_string(),
            oidc_userinfo_endpoint: "https://issuer.example.test/userinfo".to_string(),
            oidc_client_id: "client-id".to_string(),
            oidc_client_secret: "super-secret-value".to_string(),
            oidc_scopes: "openid profile email".to_string(),
            oidc_claim_email: "email".to_string(),
            oidc_claim_display_name: "name".to_string(),
            oidc_claim_subject: "sub".to_string(),
        }
    }

    #[test]
    fn authorization_url_contains_required_parameters() {
        let url = authorization_url(&settings(), "https://admin.example.test").unwrap();
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=client-id"));
        assert!(url.contains("scope=openid+profile+email"));
        assert!(url.contains("state="));
    }

    #[test]
    fn generated_state_is_accepted_for_matching_origin() {
        let url = authorization_url(&settings(), "https://admin.example.test").unwrap();
        let state = url
            .split("state=")
            .nth(1)
            .and_then(|tail| tail.split('&').next())
            .unwrap();
        verify_state(
            state,
            "super-secret-value",
            "https://admin.example.test/api/auth/oidc/callback",
        )
        .unwrap();
    }
}
