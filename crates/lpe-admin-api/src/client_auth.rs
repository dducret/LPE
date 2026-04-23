use axum::{
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::Redirect,
    Json,
};
use std::collections::HashMap;
use uuid::Uuid;

use lpe_mail_auth::{issue_oauth_access_token, normalize_scope, DEFAULT_OAUTH_ACCESS_SCOPE};
use lpe_storage::{AuditEntryInput, AuthenticatedAccount, HealthResponse, Storage};

use crate::{
    account_oidc,
    http::{bad_request_error, internal_error, public_origin},
    require_account,
    security::{
        client_oauth_access_token_seconds, client_session_minutes, generate_app_password_secret,
        hash_password, verify_password,
    },
    totp,
    types::{
        AccountAppPasswordsResponse, AccountAuthFactorsResponse, ApiResult, ClientLoginResponse,
        ClientOauthAccessTokenResponse, ClientOidcMetadataResponse, ClientOidcStartResponse,
        CreateAccountAppPasswordRequest, CreateAccountAppPasswordResponse,
        CreateClientOauthAccessTokenRequest, EnrollTotpRequest, EnrollTotpResponse, LoginRequest,
        VerifyTotpRequest,
    },
};

pub(crate) async fn client_login(
    State(storage): State<Storage>,
    Json(request): Json<LoginRequest>,
) -> ApiResult<ClientLoginResponse> {
    let security = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?
        .security_settings;
    if !security.mailbox_password_login_enabled {
        return Err((
            StatusCode::FORBIDDEN,
            "password login is disabled for mailbox accounts".to_string(),
        ));
    }

    let email = request.email.trim().to_lowercase();
    let candidate = storage
        .fetch_account_login(&email)
        .await
        .map_err(internal_error)?
        .ok_or((StatusCode::UNAUTHORIZED, "invalid credentials".to_string()))?;

    if candidate.status != "active" || !verify_password(&candidate.password_hash, &request.password)
    {
        let _ = storage
            .append_audit_event(
                &candidate.tenant_id,
                AuditEntryInput {
                    actor: email.clone(),
                    action: "mail-auth.password-login-failed".to_string(),
                    subject: "invalid-credentials".to_string(),
                },
            )
            .await;
        return Err((StatusCode::UNAUTHORIZED, "invalid credentials".to_string()));
    }

    let auth_method = if let Some((_, secret)) = storage
        .fetch_account_totp_secret(&email)
        .await
        .map_err(internal_error)?
    {
        let code = request
            .totp_code
            .as_deref()
            .ok_or((StatusCode::UNAUTHORIZED, "missing TOTP code".to_string()))?;
        if !totp::verify_code(&secret, code, totp::unix_time()) {
            let _ = storage
                .append_audit_event(
                    &candidate.tenant_id,
                    AuditEntryInput {
                        actor: email.clone(),
                        action: "mail-auth.totp-failed".to_string(),
                        subject: "password".to_string(),
                    },
                )
                .await;
            return Err((StatusCode::UNAUTHORIZED, "invalid TOTP code".to_string()));
        }
        "password+totp"
    } else {
        "password"
    };

    let token = Uuid::new_v4().to_string();
    storage
        .create_account_session(
            &token,
            &candidate.tenant_id,
            &candidate.email,
            client_session_minutes(),
        )
        .await
        .map_err(internal_error)?;
    let _ = storage
        .append_audit_event(
            &candidate.tenant_id,
            AuditEntryInput {
                actor: email.clone(),
                action: "mail-auth.login-succeeded".to_string(),
                subject: auth_method.to_string(),
            },
        )
        .await;
    let account = storage
        .fetch_account_session(&token)
        .await
        .map_err(internal_error)?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "session creation failed".to_string(),
        ))?;

    Ok(Json(ClientLoginResponse { token, account }))
}

pub(crate) async fn client_logout(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<HealthResponse> {
    if let Some(token) = crate::http::bearer_token(&headers) {
        storage
            .delete_account_session(&token)
            .await
            .map_err(internal_error)?;
    }
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn client_me(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<AuthenticatedAccount> {
    Ok(Json(require_account(&storage, &headers).await?))
}

pub(crate) async fn account_auth_factors(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<AccountAuthFactorsResponse> {
    let account = require_account(&storage, &headers).await?;
    let factors = storage
        .fetch_account_auth_factors(&account.email)
        .await
        .map_err(internal_error)?;
    Ok(Json(AccountAuthFactorsResponse { factors }))
}

pub(crate) async fn enroll_account_totp(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<EnrollTotpRequest>,
) -> ApiResult<EnrollTotpResponse> {
    let account = require_account(&storage, &headers).await?;
    let dashboard = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    let label = request
        .label
        .unwrap_or_else(|| account.display_name.clone())
        .trim()
        .to_string();
    let secret = totp::generate_secret();
    let factor_id = storage
        .create_account_auth_factor(lpe_storage::NewAccountAuthFactor {
            account_email: account.email.clone(),
            factor_type: "totp".to_string(),
            secret_ciphertext: secret.clone(),
        })
        .await
        .map_err(internal_error)?;
    let _ = storage
        .append_audit_event(
            &account.tenant_id,
            AuditEntryInput {
                actor: account.email.clone(),
                action: "mail-auth.totp-enrollment-started".to_string(),
                subject: factor_id.to_string(),
            },
        )
        .await;
    Ok(Json(EnrollTotpResponse {
        factor_id,
        secret: secret.clone(),
        otpauth_url: totp::otpauth_url(
            &dashboard.server_settings.primary_hostname,
            &account.email,
            &label,
            &secret,
        ),
    }))
}

pub(crate) async fn verify_account_totp_factor(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<VerifyTotpRequest>,
) -> ApiResult<AccountAuthFactorsResponse> {
    let account = require_account(&storage, &headers).await?;
    let secret = storage
        .fetch_pending_account_factor_secret(&account.email, request.factor_id)
        .await
        .map_err(internal_error)?
        .ok_or((
            StatusCode::NOT_FOUND,
            "pending factor not found".to_string(),
        ))?;
    if !totp::verify_code(&secret, &request.code, totp::unix_time()) {
        return Err((StatusCode::UNAUTHORIZED, "invalid TOTP code".to_string()));
    }
    storage
        .activate_account_auth_factor(&account.email, request.factor_id)
        .await
        .map_err(internal_error)?;
    let _ = storage
        .append_audit_event(
            &account.tenant_id,
            AuditEntryInput {
                actor: account.email.clone(),
                action: "mail-auth.totp-enrollment-verified".to_string(),
                subject: request.factor_id.to_string(),
            },
        )
        .await;
    let factors = storage
        .fetch_account_auth_factors(&account.email)
        .await
        .map_err(internal_error)?;
    Ok(Json(AccountAuthFactorsResponse { factors }))
}

pub(crate) async fn revoke_account_factor(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(factor_id): AxumPath<Uuid>,
) -> ApiResult<AccountAuthFactorsResponse> {
    let account = require_account(&storage, &headers).await?;
    let revoked = storage
        .revoke_account_auth_factor(&account.email, factor_id)
        .await
        .map_err(internal_error)?;
    if !revoked {
        return Err((StatusCode::NOT_FOUND, "factor not found".to_string()));
    }
    let _ = storage
        .append_audit_event(
            &account.tenant_id,
            AuditEntryInput {
                actor: account.email.clone(),
                action: "mail-auth.factor-revoked".to_string(),
                subject: factor_id.to_string(),
            },
        )
        .await;
    let factors = storage
        .fetch_account_auth_factors(&account.email)
        .await
        .map_err(internal_error)?;
    Ok(Json(AccountAuthFactorsResponse { factors }))
}

pub(crate) async fn list_account_app_passwords(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<AccountAppPasswordsResponse> {
    let account = require_account(&storage, &headers).await?;
    let app_passwords = storage
        .list_account_app_passwords(&account.email)
        .await
        .map_err(internal_error)?;
    Ok(Json(AccountAppPasswordsResponse { app_passwords }))
}

pub(crate) async fn create_account_app_password(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateAccountAppPasswordRequest>,
) -> ApiResult<CreateAccountAppPasswordResponse> {
    let account = require_account(&storage, &headers).await?;
    let label = request.label.trim();
    if label.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "label is required".to_string()));
    }
    let secret = generate_app_password_secret();
    let id = storage
        .create_account_app_password(
            &account.email,
            label,
            &hash_password(&secret).map_err(internal_error)?,
        )
        .await
        .map_err(internal_error)?;
    let _ = storage
        .append_audit_event(
            &account.tenant_id,
            AuditEntryInput {
                actor: account.email.clone(),
                action: "mail-auth.app-password-created".to_string(),
                subject: label.to_string(),
            },
        )
        .await;
    Ok(Json(CreateAccountAppPasswordResponse {
        id,
        label: label.to_string(),
        secret,
    }))
}

pub(crate) async fn revoke_account_app_password(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(app_password_id): AxumPath<Uuid>,
) -> ApiResult<AccountAppPasswordsResponse> {
    let account = require_account(&storage, &headers).await?;
    let revoked = storage
        .revoke_account_app_password(&account.email, app_password_id)
        .await
        .map_err(internal_error)?;
    if !revoked {
        return Err((StatusCode::NOT_FOUND, "app password not found".to_string()));
    }
    let _ = storage
        .append_audit_event(
            &account.tenant_id,
            AuditEntryInput {
                actor: account.email.clone(),
                action: "mail-auth.app-password-revoked".to_string(),
                subject: app_password_id.to_string(),
            },
        )
        .await;
    let app_passwords = storage
        .list_account_app_passwords(&account.email)
        .await
        .map_err(internal_error)?;
    Ok(Json(AccountAppPasswordsResponse { app_passwords }))
}

pub(crate) async fn create_client_oauth_access_token(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateClientOauthAccessTokenRequest>,
) -> ApiResult<ClientOauthAccessTokenResponse> {
    let account = require_account(&storage, &headers).await?;
    let scope = normalize_scope(
        request
            .scope
            .as_deref()
            .unwrap_or(DEFAULT_OAUTH_ACCESS_SCOPE),
    )
    .map_err(bad_request_error)?;
    let expires_in = client_oauth_access_token_seconds();
    let access_token = issue_oauth_access_token(
        &lpe_mail_auth::AccountPrincipal {
            tenant_id: account.tenant_id.clone(),
            account_id: account.account_id,
            email: account.email.clone(),
            display_name: account.display_name.clone(),
        },
        &scope,
        expires_in,
    )
    .map_err(internal_error)?;
    let _ = storage
        .append_audit_event(
            &account.tenant_id,
            AuditEntryInput {
                actor: account.email.clone(),
                action: "mail-auth.oauth-access-token-created".to_string(),
                subject: scope.clone(),
            },
        )
        .await;

    Ok(Json(ClientOauthAccessTokenResponse {
        access_token,
        token_type: "Bearer".to_string(),
        expires_in,
        scope,
    }))
}

pub(crate) async fn client_oidc_metadata(
    State(storage): State<Storage>,
) -> ApiResult<ClientOidcMetadataResponse> {
    let settings = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?
        .security_settings;
    Ok(Json(ClientOidcMetadataResponse {
        enabled: settings.mailbox_oidc_login_enabled,
        provider_label: settings.mailbox_oidc_provider_label,
    }))
}

pub(crate) async fn client_oidc_start(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<ClientOidcStartResponse> {
    let settings = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?
        .security_settings;
    let public_origin = public_origin(&headers);
    let authorization_url = account_oidc::authorization_url(&settings, &public_origin)
        .await
        .map_err(bad_request_error)?;
    Ok(Json(ClientOidcStartResponse { authorization_url }))
}

pub(crate) async fn client_oidc_callback(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Redirect, (StatusCode, String)> {
    let code = params
        .get("code")
        .cloned()
        .ok_or((StatusCode::BAD_REQUEST, "missing OIDC code".to_string()))?;
    let state = params
        .get("state")
        .cloned()
        .ok_or((StatusCode::BAD_REQUEST, "missing OIDC state".to_string()))?;

    let settings = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?
        .security_settings;
    let public_origin = public_origin(&headers);
    let claims = account_oidc::exchange_code_for_claims(&settings, &public_origin, &code, &state)
        .await
        .map_err(bad_request_error)?;

    let account_email = match storage
        .find_account_oidc_identity(&claims.issuer_url, &claims.subject)
        .await
        .map_err(internal_error)?
    {
        Some(email) => email,
        None if settings.mailbox_oidc_auto_link_by_email => storage
            .fetch_account_login(&claims.email)
            .await
            .map_err(internal_error)?
            .map(|candidate| candidate.email)
            .ok_or((
                StatusCode::FORBIDDEN,
                "OIDC identity is not linked to a mailbox account".to_string(),
            ))?,
        None => {
            return Err((
                StatusCode::FORBIDDEN,
                "OIDC identity is not linked to a mailbox account".to_string(),
            ));
        }
    };

    storage
        .upsert_account_oidc_identity(&lpe_storage::AccountOidcClaims {
            issuer_url: claims.issuer_url,
            subject: claims.subject,
            email: account_email.clone(),
            display_name: claims.display_name,
        })
        .await
        .map_err(internal_error)?;

    let account = storage
        .fetch_account_login(&account_email)
        .await
        .map_err(internal_error)?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "mailbox account not found".to_string(),
        ))?;
    let token = Uuid::new_v4().to_string();
    storage
        .create_account_session(
            &token,
            &account.tenant_id,
            &account.email,
            client_session_minutes(),
        )
        .await
        .map_err(internal_error)?;
    let _ = storage
        .append_audit_event(
            &account.tenant_id,
            AuditEntryInput {
                actor: account.email.clone(),
                action: "mail-auth.login-succeeded".to_string(),
                subject: "oidc".to_string(),
            },
        )
        .await;
    Ok(Redirect::to(&format!("/mail/#client_token={token}")))
}
