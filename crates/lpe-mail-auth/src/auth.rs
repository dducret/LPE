use anyhow::{anyhow, bail, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use axum::http::HeaderMap;
use lpe_domain::normalization;
use lpe_storage::AuditEntryInput;

use crate::{
    oauth::{
        basic_credentials, bearer_token, decode_oauth_access_token, scope_allows_surface,
        AccountPrincipal,
    },
    store::AccountAuthStore,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountAuthenticationMethod {
    Session,
    OAuth,
    Password,
    AppPassword,
}

impl AccountAuthenticationMethod {
    pub fn audit_subject(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::OAuth => "oauth",
            Self::Password => "password",
            Self::AppPassword => "app-password",
        }
    }

    fn existing_flow_records_success(self) -> bool {
        matches!(self, Self::Password | Self::AppPassword)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum AccountAuthenticationVerifier {
    None,
    PasswordHash(String),
    AppPassword {
        id: uuid::Uuid,
        password_hash: String,
    },
}

impl std::fmt::Debug for AccountAuthenticationVerifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::PasswordHash(_) => formatter.write_str("PasswordHash([redacted])"),
            Self::AppPassword { id, .. } => formatter
                .debug_struct("AppPassword")
                .field("id", id)
                .field("password_hash", &"[redacted]")
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedAccountAuthentication {
    pub principal: AccountPrincipal,
    pub method: AccountAuthenticationMethod,
    pub verifier: AccountAuthenticationVerifier,
}

pub async fn authenticate_account<S: AccountAuthStore>(
    store: &S,
    hinted_user: Option<&str>,
    headers: &HeaderMap,
    surface: &str,
) -> Result<AccountPrincipal> {
    let authentication =
        verify_account_authentication(store, hinted_user, headers, surface).await?;
    if authentication.method.existing_flow_records_success() {
        record_account_login_success(store, &authentication, surface).await;
    }
    Ok(authentication.principal)
}

pub async fn verify_account_authentication<S: AccountAuthStore>(
    store: &S,
    hinted_user: Option<&str>,
    headers: &HeaderMap,
    surface: &str,
) -> Result<VerifiedAccountAuthentication> {
    verify_account_authentication_inner(store, hinted_user, headers, surface).await
}

async fn verify_account_authentication_inner<S: AccountAuthStore>(
    store: &S,
    hinted_user: Option<&str>,
    headers: &HeaderMap,
    surface: &str,
) -> Result<VerifiedAccountAuthentication> {
    if let Some(token) = bearer_token(headers) {
        if let Some(account) = store.fetch_account_session(&token).await? {
            return Ok(VerifiedAccountAuthentication {
                principal: AccountPrincipal {
                    tenant_id: account.tenant_id,
                    account_id: account.account_id,
                    email: account.email,
                    display_name: account.display_name,
                    quota_mb: None,
                    quota_used_octets: None,
                },
                method: AccountAuthenticationMethod::Session,
                verifier: AccountAuthenticationVerifier::None,
            });
        }

        if let Ok(principal) =
            authenticate_bearer_access_token(store, hinted_user, &token, surface).await
        {
            return Ok(VerifiedAccountAuthentication {
                principal,
                method: AccountAuthenticationMethod::OAuth,
                verifier: AccountAuthenticationVerifier::None,
            });
        }
    }

    if let Some((username, password)) = basic_credentials(headers)? {
        return verify_plain_credentials(store, hinted_user, &username, &password, surface).await;
    }

    bail!("missing account authentication");
}

pub async fn record_account_login_success<S: AccountAuthStore>(
    store: &S,
    authentication: &VerifiedAccountAuthentication,
    surface: &str,
) {
    if let AccountAuthenticationVerifier::AppPassword { id, .. } = &authentication.verifier {
        let _ = store
            .touch_account_app_password(&authentication.principal.email, *id)
            .await;
    }
    let _ = store
        .append_audit_event(
            &authentication.principal.tenant_id,
            AuditEntryInput {
                actor: authentication.principal.email.clone(),
                action: format!("mail-auth.{surface}.login-succeeded"),
                subject: authentication.method.audit_subject().to_string(),
            },
        )
        .await;
}

pub async fn authenticate_bearer_access_token<S: AccountAuthStore>(
    store: &S,
    hinted_user: Option<&str>,
    token: &str,
    surface: &str,
) -> Result<AccountPrincipal> {
    let claims = decode_oauth_access_token(token)?;
    if !scope_allows_surface(&claims.scope, surface) {
        bail!("oauth access token is not valid for this surface");
    }
    if let Some(hinted_user) = hinted_user {
        let hinted = normalize_login_name(hinted_user, None);
        if hinted != claims.email {
            bail!("oauth access token user does not match the requested account");
        }
    }

    let login = store
        .fetch_account_login(&claims.email)
        .await?
        .ok_or_else(|| anyhow!("invalid credentials"))?;
    if login.status != "active"
        || login.tenant_id != claims.tenant_id
        || login.account_id != claims.account_id
    {
        bail!("invalid credentials");
    }

    Ok(AccountPrincipal {
        tenant_id: login.tenant_id,
        account_id: login.account_id,
        email: login.email,
        display_name: login.display_name,
        quota_mb: Some(login.quota_mb),
        quota_used_octets: Some(login.quota_used_octets),
    })
}

pub async fn authenticate_plain_credentials<S: AccountAuthStore>(
    store: &S,
    hinted_user: Option<&str>,
    username: &str,
    password: &str,
    surface: &str,
) -> Result<AccountPrincipal> {
    let authentication =
        verify_plain_credentials(store, hinted_user, username, password, surface).await?;
    record_account_login_success(store, &authentication, surface).await;
    Ok(authentication.principal)
}

async fn verify_plain_credentials<S: AccountAuthStore>(
    store: &S,
    hinted_user: Option<&str>,
    username: &str,
    password: &str,
    surface: &str,
) -> Result<VerifiedAccountAuthentication> {
    let normalized = normalize_login_name(username, hinted_user);
    let login = store
        .fetch_account_login(&normalized)
        .await?
        .ok_or_else(|| anyhow!("invalid credentials"))?;

    if login.status != "active" {
        let _ = store
            .append_audit_event(
                &login.tenant_id,
                AuditEntryInput {
                    actor: normalized.clone(),
                    action: format!("mail-auth.{surface}.login-failed"),
                    subject: "inactive-account".to_string(),
                },
            )
            .await;
        bail!("invalid credentials");
    }

    let (auth_method, verifier) = if verify_password(&login.password_hash, password) {
        (
            AccountAuthenticationMethod::Password,
            AccountAuthenticationVerifier::PasswordHash(login.password_hash.clone()),
        )
    } else {
        let app_passwords = store
            .fetch_active_account_app_passwords(&normalized)
            .await?;
        let Some(app_password) = app_passwords
            .into_iter()
            .find(|entry| verify_password(&entry.password_hash, password))
        else {
            let _ = store
                .append_audit_event(
                    &login.tenant_id,
                    AuditEntryInput {
                        actor: normalized.clone(),
                        action: format!("mail-auth.{surface}.login-failed"),
                        subject: "invalid-credentials".to_string(),
                    },
                )
                .await;
            bail!("invalid credentials");
        };
        (
            AccountAuthenticationMethod::AppPassword,
            AccountAuthenticationVerifier::AppPassword {
                id: app_password.id,
                password_hash: app_password.password_hash,
            },
        )
    };

    Ok(VerifiedAccountAuthentication {
        principal: AccountPrincipal {
            tenant_id: login.tenant_id,
            account_id: login.account_id,
            email: login.email,
            display_name: login.display_name,
            quota_mb: Some(login.quota_mb),
            quota_used_octets: Some(login.quota_used_octets),
        },
        method: auth_method,
        verifier,
    })
}

pub fn normalize_login_name(username: &str, hinted_user: Option<&str>) -> String {
    normalization::normalize_login_name(username, hinted_user)
}

pub fn verify_password(password_hash: &str, password: &str) -> bool {
    PasswordHash::new(password_hash)
        .ok()
        .and_then(|parsed| {
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .ok()
        })
        .is_some()
}
