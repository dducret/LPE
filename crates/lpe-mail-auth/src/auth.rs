use anyhow::{anyhow, bail, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use axum::http::HeaderMap;
use lpe_storage::AuditEntryInput;

use crate::{
    oauth::{
        basic_credentials, bearer_token, decode_oauth_access_token, scope_allows_surface,
        AccountPrincipal,
    },
    store::AccountAuthStore,
};

pub async fn authenticate_account<S: AccountAuthStore>(
    store: &S,
    hinted_user: Option<&str>,
    headers: &HeaderMap,
    surface: &str,
) -> Result<AccountPrincipal> {
    if let Some(token) = bearer_token(headers) {
        if let Some(account) = store.fetch_account_session(&token).await? {
            return Ok(AccountPrincipal {
                tenant_id: account.tenant_id,
                account_id: account.account_id,
                email: account.email,
                display_name: account.display_name,
            });
        }

        if let Ok(principal) =
            authenticate_bearer_access_token(store, hinted_user, &token, surface).await
        {
            return Ok(principal);
        }
    }

    if let Some((username, password)) = basic_credentials(headers)? {
        return authenticate_plain_credentials(store, hinted_user, &username, &password, surface)
            .await;
    }

    bail!("missing account authentication");
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
    })
}

pub async fn authenticate_plain_credentials<S: AccountAuthStore>(
    store: &S,
    hinted_user: Option<&str>,
    username: &str,
    password: &str,
    surface: &str,
) -> Result<AccountPrincipal> {
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

    let auth_method = if verify_password(&login.password_hash, password) {
        "password".to_string()
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
        let _ = store
            .touch_account_app_password(&normalized, app_password.id)
            .await;
        "app-password".to_string()
    };

    let _ = store
        .append_audit_event(
            &login.tenant_id,
            AuditEntryInput {
                actor: login.email.clone(),
                action: format!("mail-auth.{surface}.login-succeeded"),
                subject: auth_method,
            },
        )
        .await;

    Ok(AccountPrincipal {
        tenant_id: login.tenant_id,
        account_id: login.account_id,
        email: login.email,
        display_name: login.display_name,
    })
}

pub fn normalize_login_name(username: &str, hinted_user: Option<&str>) -> String {
    if username.contains('@') {
        username.trim().to_lowercase()
    } else {
        hinted_user.unwrap_or(username).trim().to_lowercase()
    }
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
