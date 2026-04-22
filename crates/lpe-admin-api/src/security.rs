use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use lpe_mail_auth::DEFAULT_OAUTH_ACCESS_TOKEN_SECONDS;
use std::env;
use uuid::Uuid;

pub(crate) fn generate_app_password_secret() -> String {
    format!(
        "lpeapp-{}-{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

pub(crate) fn admin_session_minutes() -> u32 {
    env::var("LPE_ADMIN_SESSION_MINUTES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(45)
}

pub(crate) fn client_session_minutes() -> u32 {
    env::var("LPE_CLIENT_SESSION_MINUTES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(720)
}

pub(crate) fn client_oauth_access_token_seconds() -> u32 {
    env::var("LPE_MAIL_OAUTH_ACCESS_TOKEN_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value >= 60)
        .unwrap_or(DEFAULT_OAUTH_ACCESS_TOKEN_SECONDS)
}

pub(crate) fn hash_password(password: &str) -> anyhow::Result<String> {
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
