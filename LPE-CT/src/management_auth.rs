use super::*;
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};

const MIN_INTEGRATION_SECRET_LEN: usize = 32;
static INTEGRATION_REPLAY_CACHE: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<String, i64>>,
> = std::sync::OnceLock::new();

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, self.message).into_response()
    }
}

pub(crate) fn require_management_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<ManagementSession, ApiError> {
    let token = bearer_token(headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing bearer token"))?;
    let session = state
        .sessions
        .lock()
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "session lock poisoned"))?
        .get(&token)
        .cloned()
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "invalid management session"))?;
    Ok(session)
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

pub(crate) fn require_integration_request<T: serde::Serialize>(
    headers: &HeaderMap,
    path: &str,
    payload: &T,
) -> Result<(), ApiError> {
    let provided = headers
        .get(INTEGRATION_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            observability::record_security_event("integration_auth_failure");
            ApiError::new(StatusCode::UNAUTHORIZED, "missing integration key")
        })?;
    let expected = integration_shared_secret()
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if provided != expected {
        observability::record_security_event("integration_auth_failure");
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid integration key",
        ));
    }
    let signed = SignedIntegrationHeaders {
        integration_key: provided.to_string(),
        timestamp: required_header(headers, INTEGRATION_TIMESTAMP_HEADER)?,
        nonce: required_header(headers, INTEGRATION_NONCE_HEADER)?,
        signature: required_header(headers, INTEGRATION_SIGNATURE_HEADER)?,
    };
    signed
        .validate_payload(
            &expected,
            "POST",
            path,
            payload,
            current_unix_timestamp(),
            DEFAULT_MAX_SKEW_SECONDS,
        )
        .map_err(integration_auth_api_error)?;
    ensure_not_replayed(&signed).map_err(integration_auth_api_error)?;
    Ok(())
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| integration_auth_api_error(BridgeAuthError::MissingHeader(name)))
}

fn ensure_not_replayed(signed: &SignedIntegrationHeaders) -> Result<(), BridgeAuthError> {
    let cache = INTEGRATION_REPLAY_CACHE
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()));
    let now = current_unix_timestamp();
    let mut guard = cache.lock().map_err(|_| {
        BridgeAuthError::InvalidPayload("integration replay cache lock poisoned".to_string())
    })?;
    let cutoff = now - DEFAULT_MAX_SKEW_SECONDS;
    guard.retain(|_, seen_at| *seen_at >= cutoff);
    let key = signed.replay_key();
    if guard.insert(key, now).is_some() {
        return Err(BridgeAuthError::InvalidPayload(
            "integration request replay detected".to_string(),
        ));
    }
    Ok(())
}

fn integration_auth_api_error(error: BridgeAuthError) -> ApiError {
    observability::record_security_event("integration_auth_failure");
    ApiError::new(StatusCode::UNAUTHORIZED, error.to_string())
}

pub(crate) fn integration_shared_secret() -> Result<String> {
    let value = env::var("LPE_INTEGRATION_SHARED_SECRET")
        .map_err(|_| anyhow::anyhow!("LPE_INTEGRATION_SHARED_SECRET must be set"))?;
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        anyhow::bail!("LPE_INTEGRATION_SHARED_SECRET must not be empty");
    }
    if trimmed.len() < MIN_INTEGRATION_SECRET_LEN {
        anyhow::bail!(
            "LPE_INTEGRATION_SHARED_SECRET must contain at least {MIN_INTEGRATION_SECRET_LEN} characters"
        );
    }
    if is_known_weak_secret(&trimmed) {
        anyhow::bail!("LPE_INTEGRATION_SHARED_SECRET uses a forbidden weak placeholder value");
    }
    Ok(trimmed)
}

pub(crate) fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::encode_b64(Uuid::new_v4().as_bytes())
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .to_string())
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

pub(crate) fn is_known_weak_secret(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "change-me"
            | "changeme"
            | "secret"
            | "shared-secret"
            | "integration-test"
            | "password"
            | "admin"
            | "default"
            | "test"
            | "example"
    )
}
