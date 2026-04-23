use axum::{
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::Redirect,
    Json,
};
use std::collections::HashMap;
use uuid::Uuid;

use lpe_storage::{
    AuditEntryInput, AuthenticatedAdmin, HealthResponse, NewAdminAuthFactor, Storage,
};

use crate::{
    http::{bad_request_error, bearer_token, internal_error, public_origin},
    oidc, require_admin,
    security::{admin_session_minutes, verify_password},
    totp,
    types::{
        AdminAuthFactorsResponse, ApiResult, EnrollTotpRequest, EnrollTotpResponse, LoginRequest,
        LoginResponse, OidcMetadataResponse, OidcStartResponse, VerifyTotpRequest,
    },
};

pub(crate) async fn login(
    State(storage): State<Storage>,
    Json(request): Json<LoginRequest>,
) -> ApiResult<LoginResponse> {
    let security = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?
        .security_settings;
    if !security.password_login_enabled {
        return Err((
            StatusCode::FORBIDDEN,
            "password login is disabled for administrators".to_string(),
        ));
    }

    let email = request.email.trim().to_lowercase();
    let candidate = storage
        .fetch_admin_login(&email)
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
                    action: "admin-auth.login-failed".to_string(),
                    subject: "password".to_string(),
                },
            )
            .await;
        return Err((StatusCode::UNAUTHORIZED, "invalid credentials".to_string()));
    }

    let auth_method = if let Some((_, secret)) = storage
        .fetch_admin_totp_secret(&email)
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
                        action: "admin-auth.totp-failed".to_string(),
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
        .create_admin_session(
            &token,
            &candidate.tenant_id,
            &email,
            admin_session_minutes(),
            auth_method,
        )
        .await
        .map_err(internal_error)?;
    let _ = storage
        .append_audit_event(
            &candidate.tenant_id,
            AuditEntryInput {
                actor: email.clone(),
                action: "admin-auth.login-succeeded".to_string(),
                subject: auth_method.to_string(),
            },
        )
        .await;
    let admin = storage
        .fetch_admin_session(&token)
        .await
        .map_err(internal_error)?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "session creation failed".to_string(),
        ))?;

    Ok(Json(LoginResponse { token, admin }))
}

pub(crate) async fn logout(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<HealthResponse> {
    if let Some(token) = bearer_token(&headers) {
        if let Ok(Some(admin)) = storage.fetch_admin_session(&token).await {
            let _ = storage
                .append_audit_event(
                    &admin.tenant_id,
                    AuditEntryInput {
                        actor: admin.email.clone(),
                        action: "admin-auth.logout".to_string(),
                        subject: admin.auth_method.clone(),
                    },
                )
                .await;
        }
        storage
            .delete_admin_session(&token)
            .await
            .map_err(internal_error)?;
    }
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn me(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<AuthenticatedAdmin> {
    Ok(Json(require_admin(&storage, &headers, "dashboard").await?))
}

pub(crate) async fn admin_auth_factors(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<AdminAuthFactorsResponse> {
    let admin = require_admin(&storage, &headers, "dashboard").await?;
    let factors = storage
        .fetch_admin_auth_factors(&admin.email)
        .await
        .map_err(internal_error)?;
    Ok(Json(AdminAuthFactorsResponse { factors }))
}

pub(crate) async fn enroll_totp(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<EnrollTotpRequest>,
) -> ApiResult<EnrollTotpResponse> {
    let admin = require_admin(&storage, &headers, "dashboard").await?;
    let dashboard = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    let label = request
        .label
        .unwrap_or_else(|| admin.display_name.clone())
        .trim()
        .to_string();
    let secret = totp::generate_secret();
    let factor_id = storage
        .create_admin_auth_factor(NewAdminAuthFactor {
            admin_email: admin.email.clone(),
            factor_type: "totp".to_string(),
            secret_ciphertext: secret.clone(),
        })
        .await
        .map_err(internal_error)?;
    let _ = storage
        .append_audit_event(
            &admin.tenant_id,
            AuditEntryInput {
                actor: admin.email.clone(),
                action: "admin-auth.totp-enrollment-started".to_string(),
                subject: factor_id.to_string(),
            },
        )
        .await;
    Ok(Json(EnrollTotpResponse {
        factor_id,
        secret: secret.clone(),
        otpauth_url: totp::otpauth_url(
            &dashboard.server_settings.primary_hostname,
            &admin.email,
            &label,
            &secret,
        ),
    }))
}

pub(crate) async fn verify_totp_factor(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<VerifyTotpRequest>,
) -> ApiResult<AdminAuthFactorsResponse> {
    let admin = require_admin(&storage, &headers, "dashboard").await?;
    let secret = storage
        .fetch_pending_admin_factor_secret(&admin.email, request.factor_id)
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
        .activate_admin_auth_factor(&admin.email, request.factor_id)
        .await
        .map_err(internal_error)?;
    let _ = storage
        .append_audit_event(
            &admin.tenant_id,
            AuditEntryInput {
                actor: admin.email.clone(),
                action: "admin-auth.totp-enrollment-verified".to_string(),
                subject: request.factor_id.to_string(),
            },
        )
        .await;
    let factors = storage
        .fetch_admin_auth_factors(&admin.email)
        .await
        .map_err(internal_error)?;
    Ok(Json(AdminAuthFactorsResponse { factors }))
}

pub(crate) async fn revoke_admin_factor(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(factor_id): AxumPath<Uuid>,
) -> ApiResult<AdminAuthFactorsResponse> {
    let admin = require_admin(&storage, &headers, "dashboard").await?;
    let revoked = storage
        .revoke_admin_auth_factor(&admin.email, factor_id)
        .await
        .map_err(internal_error)?;
    if !revoked {
        return Err((StatusCode::NOT_FOUND, "factor not found".to_string()));
    }
    let _ = storage
        .append_audit_event(
            &admin.tenant_id,
            AuditEntryInput {
                actor: admin.email.clone(),
                action: "admin-auth.factor-revoked".to_string(),
                subject: factor_id.to_string(),
            },
        )
        .await;
    let factors = storage
        .fetch_admin_auth_factors(&admin.email)
        .await
        .map_err(internal_error)?;
    Ok(Json(AdminAuthFactorsResponse { factors }))
}

pub(crate) async fn oidc_metadata(
    State(storage): State<Storage>,
) -> ApiResult<OidcMetadataResponse> {
    let settings = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?
        .security_settings;
    Ok(Json(OidcMetadataResponse {
        enabled: settings.oidc_login_enabled,
        provider_label: settings.oidc_provider_label,
    }))
}

pub(crate) async fn oidc_start(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<OidcStartResponse> {
    let settings = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?
        .security_settings;
    let public_origin = public_origin(&headers);
    let authorization_url = oidc::authorization_url(&settings, &public_origin)
        .await
        .map_err(bad_request_error)?;
    Ok(Json(OidcStartResponse { authorization_url }))
}

pub(crate) async fn oidc_callback(
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
    let claims = oidc::exchange_code_for_claims(&settings, &public_origin, &code, &state)
        .await
        .map_err(bad_request_error)?;

    let admin_email = match storage
        .find_admin_oidc_identity(&claims.issuer_url, &claims.subject)
        .await
        .map_err(internal_error)?
    {
        Some(email) => email,
        None if settings.oidc_auto_link_by_email => {
            let admin = storage
                .find_server_administrator_by_email(&claims.email)
                .await
                .map_err(internal_error)?
                .ok_or((
                    StatusCode::FORBIDDEN,
                    "OIDC identity is not linked to an administrator".to_string(),
                ))?;
            storage
                .upsert_admin_oidc_identity(&lpe_storage::AdminOidcClaims {
                    issuer_url: claims.issuer_url.clone(),
                    subject: claims.subject.clone(),
                    email: admin.email.clone(),
                    display_name: claims.display_name.clone(),
                })
                .await
                .map_err(internal_error)?;
            admin.email
        }
        None => {
            return Err((
                StatusCode::FORBIDDEN,
                "OIDC identity is not linked to an administrator".to_string(),
            ));
        }
    };

    storage
        .ensure_admin_credential_stub(&admin_email)
        .await
        .map_err(internal_error)?;

    let token = Uuid::new_v4().to_string();
    storage
        .create_admin_session(
            &token,
            &storage
                .fetch_admin_login(&admin_email)
                .await
                .map_err(internal_error)?
                .ok_or((
                    StatusCode::UNAUTHORIZED,
                    "administrator not found".to_string(),
                ))?
                .tenant_id,
            &admin_email,
            admin_session_minutes(),
            "oidc",
        )
        .await
        .map_err(internal_error)?;

    let redirect = format!("/#admin_token={token}");
    Ok(Redirect::to(&redirect))
}
