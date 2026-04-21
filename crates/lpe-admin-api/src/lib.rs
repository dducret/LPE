use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    middleware,
    response::Redirect,
    routing::{delete, get, post, put},
    Json, Router,
};
use lpe_ai::{LocalModelProvider, StubLocalModelProvider};
use lpe_core::CoreService;
use lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, SmtpSubmissionAuthRequest,
    SmtpSubmissionAuthResponse, SmtpSubmissionRequest, SmtpSubmissionResponse,
};
use lpe_magika::{
    collect_mime_attachment_parts, write_validation_record, Detector, ExpectedKind, IngressContext,
    PolicyDecision, ValidationRequest, Validator,
};
use lpe_mail_auth::{
    authenticate_plain_credentials, issue_oauth_access_token, normalize_scope, AccountPrincipal,
    DEFAULT_OAUTH_ACCESS_SCOPE, DEFAULT_OAUTH_ACCESS_TOKEN_SECONDS,
};
use lpe_storage::{
    AccountCredentialInput, AdminCredentialInput, AdminDashboard, AuditEntryInput,
    AuthenticatedAccount, AuthenticatedAdmin, ClientContact, ClientEvent, ClientTask,
    ClientWorkspace, CollaborationGrantInput, CollaborationResourceKind, DashboardUpdate,
    EmailTraceResult, EmailTraceSearchInput, HealthResponse, LocalAiSettings,
    MailboxDelegationGrantInput, NewAccount, NewAdminAuthFactor, NewAlias, NewDomain,
    NewFilterRule, NewMailbox, NewPstTransferJob, NewServerAdministrator, PstJobExecutionSummary,
    SavedDraftMessage, SecuritySettings, SenderDelegationGrant, SenderDelegationGrantInput,
    SenderDelegationRight, ServerSettings, SieveScriptDocument, Storage,
    SubmissionAccountIdentity, SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput,
    UpdateAccount, UpdateDomain,
    UpsertClientContactInput, UpsertClientEventInput, UpsertClientTaskInput,
};
use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tracing::info;
use uuid::Uuid;

mod account_oidc;
mod client_config;
mod observability;
mod oidc;
mod totp;
mod types;

use crate::types::{
    AccountAppPasswordsResponse, AccountAuthFactorsResponse, AdminAuthFactorsResponse, ApiResult,
    AttachmentSupportResponse, BootstrapAdminRequest, BootstrapAdminResponse, ClientLoginResponse,
    ClientOauthAccessTokenResponse, ClientOidcMetadataResponse, ClientOidcStartResponse,
    CollaborationOverviewResponse, CreateAccountAppPasswordRequest,
    CreateAccountAppPasswordResponse, CreateAccountRequest, CreateAliasRequest,
    CreateClientOauthAccessTokenRequest, CreateDomainRequest, CreateFilterRuleRequest,
    CreateMailboxRequest, CreatePstTransferJobRequest, CreateServerAdministratorRequest,
    EmailTraceSearchRequest, EnrollTotpRequest, EnrollTotpResponse, LocalAiHealthResponse,
    LoginRequest, LoginResponse, MailFlowResponse, MailboxDelegationResponse, OidcMetadataResponse,
    OidcStartResponse, ReadinessCheck, ReadinessResponse, RenameSieveScriptRequest,
    SetActiveSieveScriptRequest, SieveOverviewResponse, SubmitMessageRequest,
    SubmitRecipientRequest, UpdateAccountRequest, UpdateAntispamSettingsRequest,
    UpdateDomainRequest, UpdateLocalAiSettingsRequest, UpdateSecuritySettingsRequest,
    UpdateServerSettingsRequest, UpsertClientContactRequest, UpsertClientEventRequest,
    UpsertClientTaskRequest, UpsertCollaborationGrantRequest, UpsertMailboxDelegationGrantRequest,
    UpsertSenderDelegationGrantRequest, UpsertSieveScriptRequest, VerifyTotpRequest,
};

const MIN_ADMIN_PASSWORD_LEN: usize = 12;
const MIN_INTEGRATION_SECRET_LEN: usize = 32;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .route("/metrics", get(observability::metrics_endpoint))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/factors", get(admin_auth_factors))
        .route("/auth/factors/totp/enroll", post(enroll_totp))
        .route("/auth/factors/totp/verify", post(verify_totp_factor))
        .route("/auth/factors/{factor_id}", delete(revoke_admin_factor))
        .route("/auth/oidc/metadata", get(oidc_metadata))
        .route("/auth/oidc/start", get(oidc_start))
        .route("/auth/oidc/callback", get(oidc_callback))
        .route("/mail/auth/login", post(client_login))
        .route("/mail/auth/logout", post(client_logout))
        .route("/mail/auth/me", get(client_me))
        .route("/mail/auth/factors", get(account_auth_factors))
        .route("/mail/auth/factors/totp/enroll", post(enroll_account_totp))
        .route(
            "/mail/auth/factors/totp/verify",
            post(verify_account_totp_factor),
        )
        .route(
            "/mail/auth/factors/{factor_id}",
            delete(revoke_account_factor),
        )
        .route(
            "/mail/auth/app-passwords",
            get(list_account_app_passwords).post(create_account_app_password),
        )
        .route(
            "/mail/auth/app-passwords/{app_password_id}",
            delete(revoke_account_app_password),
        )
        .route(
            "/mail/auth/oauth/access-token",
            post(create_client_oauth_access_token),
        )
        .route("/mail/auth/oidc/metadata", get(client_oidc_metadata))
        .route("/mail/auth/oidc/start", get(client_oidc_start))
        .route("/mail/auth/oidc/callback", get(client_oidc_callback))
        .route("/mail/workspace", get(client_workspace))
        .route(
            "/mail/tasks",
            get(list_client_tasks).post(upsert_client_task),
        )
        .route(
            "/mail/tasks/{task_id}",
            get(get_client_task).delete(delete_client_task),
        )
        .route("/health/local-ai", get(local_ai_health))
        .route("/capabilities/attachments", get(attachment_support))
        .route("/console/dashboard", get(dashboard))
        .route("/console/accounts", post(create_account))
        .route("/console/accounts/{account_id}", put(update_account))
        .route("/console/mailboxes", post(create_mailbox))
        .route("/console/mailboxes/pst-jobs", post(create_pst_transfer_job))
        .route(
            "/console/mailboxes/{mailbox_id}/pst-upload",
            post(upload_pst_import),
        )
        .route("/console/domains", post(create_domain))
        .route("/console/domains/{domain_id}", put(update_domain))
        .route("/console/aliases", post(create_alias))
        .route("/console/admins", post(create_server_administrator))
        .route("/console/antispam/rules", post(create_filter_rule))
        .route("/console/mail-flow", get(mail_flow))
        .route(
            "/console/mailboxes/pst-jobs/run-pending",
            post(run_pst_jobs),
        )
        .route("/mail/messages/submit", post(submit_message))
        .route("/mail/messages/draft", post(save_draft_message))
        .route(
            "/internal/lpe-ct/inbound-deliveries",
            post(deliver_inbound_message),
        )
        .route(
            "/internal/lpe-ct/submission-auth",
            post(authenticate_smtp_submission),
        )
        .route("/internal/lpe-ct/submissions", post(accept_smtp_submission))
        .route(
            "/mail/messages/{message_id}/draft",
            delete(delete_draft_message),
        )
        .route("/mail/contacts", post(upsert_client_contact))
        .route("/mail/calendar/events", post(upsert_client_event))
        .route(
            "/mail/shares",
            get(list_collaboration_overview).put(upsert_collaboration_grant),
        )
        .route(
            "/mail/shares/{kind}/{grantee_account_id}",
            delete(delete_collaboration_grant),
        )
        .route("/mail/delegation", get(get_mailbox_delegation))
        .route(
            "/mail/delegation/mailboxes",
            put(upsert_mailbox_delegation_grant),
        )
        .route(
            "/mail/delegation/mailboxes/{grantee_account_id}",
            delete(delete_mailbox_delegation_grant),
        )
        .route(
            "/mail/delegation/sender",
            put(upsert_sender_delegation_grant),
        )
        .route(
            "/mail/delegation/sender/{sender_right}/{grantee_account_id}",
            delete(delete_sender_delegation_grant),
        )
        .route(
            "/mail/sieve",
            get(get_sieve_overview).post(put_sieve_script),
        )
        .route("/mail/sieve/rename", post(rename_sieve_script))
        .route("/mail/sieve/active", put(set_active_sieve_script))
        .route(
            "/mail/sieve/{name}",
            get(get_sieve_script).delete(delete_sieve_script),
        )
        .route(
            "/console/audit/email-trace-search",
            post(search_email_trace),
        )
        .route("/console/settings/server", put(update_server_settings))
        .route("/console/settings/security", put(update_security_settings))
        .route("/console/settings/local-ai", put(update_local_ai_settings))
        .route("/console/settings/antispam", put(update_antispam_settings))
        .merge(client_config::router())
        .nest("/jmap", lpe_jmap::router())
        .merge(lpe_activesync::router())
        .merge(lpe_dav::router())
        .layer(middleware::from_fn(observability::observe_http))
        .layer(DefaultBodyLimit::max(pst_upload_max_bytes()))
        .with_state(storage)
}

async fn health(State(storage): State<Storage>) -> ApiResult<HealthResponse> {
    let dashboard = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    Ok(Json(dashboard.health))
}

async fn health_live() -> ApiResult<HealthResponse> {
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn health_ready(State(storage): State<Storage>) -> ApiResult<ReadinessResponse> {
    let mut checks = Vec::new();

    checks.push(
        match tokio::time::timeout(
            Duration::from_millis(1_500),
            storage.fetch_admin_dashboard(),
        )
        .await
        {
            Ok(Ok(_)) => readiness_ok("postgresql", true, "primary metadata store reachable"),
            Ok(Err(error)) => readiness_failed(
                "postgresql",
                true,
                format!("database-backed dashboard query failed: {error}"),
            ),
            Err(_) => readiness_failed(
                "postgresql",
                true,
                "database-backed dashboard query timed out",
            ),
        },
    );

    checks.push(match integration_shared_secret() {
        Ok(_) => readiness_ok(
            "integration-secret",
            true,
            "shared LPE/LPE-CT integration secret is configured",
        ),
        Err(error) => readiness_failed(
            "integration-secret",
            true,
            format!("integration secret is invalid: {error}"),
        ),
    });

    checks.push(ha_activation_check());

    checks.push(
        check_optional_http_dependency(
            "lpe-ct-api",
            &format!("{}/health/live", lpe_ct_base_url()),
            "outbound relay API reachable",
            "outbound relay API unreachable; outbound queue will accumulate until recovery",
        )
        .await,
    );

    Ok(Json(build_readiness_response("lpe-admin-api", checks)))
}

async fn login(
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

async fn logout(State(storage): State<Storage>, headers: HeaderMap) -> ApiResult<HealthResponse> {
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

async fn me(State(storage): State<Storage>, headers: HeaderMap) -> ApiResult<AuthenticatedAdmin> {
    Ok(Json(require_admin(&storage, &headers, "dashboard").await?))
}

async fn admin_auth_factors(
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

async fn enroll_totp(
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

async fn verify_totp_factor(
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

async fn revoke_admin_factor(
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

async fn oidc_metadata(State(storage): State<Storage>) -> ApiResult<OidcMetadataResponse> {
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

async fn oidc_start(
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

async fn oidc_callback(
    State(storage): State<Storage>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
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

async fn client_login(
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

async fn client_logout(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<HealthResponse> {
    if let Some(token) = bearer_token(&headers) {
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

async fn client_me(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<AuthenticatedAccount> {
    Ok(Json(require_account(&storage, &headers).await?))
}

async fn account_auth_factors(
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

async fn enroll_account_totp(
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

async fn verify_account_totp_factor(
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

async fn revoke_account_factor(
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

async fn list_account_app_passwords(
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

async fn create_account_app_password(
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

async fn revoke_account_app_password(
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

async fn create_client_oauth_access_token(
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

async fn client_oidc_metadata(
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

async fn client_oidc_start(
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

async fn client_oidc_callback(
    State(storage): State<Storage>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
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

async fn client_workspace(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<ClientWorkspace> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_client_workspace(account.account_id)
            .await
            .map_err(internal_error)?,
    ))
}

async fn local_ai_health(State(storage): State<Storage>) -> ApiResult<LocalAiHealthResponse> {
    let dashboard = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    let provider = StubLocalModelProvider;
    let models = provider
        .describe_models()
        .into_iter()
        .map(|model| model.id)
        .collect();
    let bootstrap_summary_payload = CoreService
        .summarize_bootstrap_projection(&provider, Uuid::new_v4())
        .map_err(internal_error)?;

    Ok(Json(LocalAiHealthResponse {
        provider: dashboard.local_ai_settings.provider,
        models,
        bootstrap_summary_payload,
        enabled: dashboard.local_ai_settings.enabled,
        offline_only: dashboard.local_ai_settings.offline_only,
    }))
}

async fn attachment_support(_: State<Storage>) -> ApiResult<AttachmentSupportResponse> {
    Ok(Json(AttachmentSupportResponse {
        formats: vec!["PDF".to_string(), "DOCX".to_string(), "ODT".to_string()],
    }))
}

async fn dashboard(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<AdminDashboard> {
    require_admin(&storage, &headers, "dashboard").await?;
    Ok(Json(
        storage
            .fetch_admin_dashboard()
            .await
            .map_err(internal_error)?,
    ))
}

async fn create_account(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateAccountRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "accounts").await?;
    ensure_admin_can_manage_email(&admin, &request.email)?;
    if request.password.trim().len() < 8 {
        return Err((
            StatusCode::BAD_REQUEST,
            "account password must contain at least 8 characters".to_string(),
        ));
    }
    let account_email = request.email.clone();
    storage
        .create_account(
            NewAccount {
                email: request.email.clone(),
                display_name: request.display_name,
                quota_mb: request.quota_mb.max(256),
                gal_visibility: request
                    .gal_visibility
                    .unwrap_or_else(|| "tenant".to_string()),
                directory_kind: request
                    .directory_kind
                    .unwrap_or_else(|| "person".to_string()),
            },
            AuditEntryInput {
                actor: admin.email.clone(),
                action: "create-account".to_string(),
                subject: "account created from admin console".to_string(),
            },
        )
        .await
        .map_err(internal_error)?;

    storage
        .upsert_account_credential(
            AccountCredentialInput {
                email: account_email.clone(),
                password_hash: hash_password(&request.password).map_err(internal_error)?,
            },
            AuditEntryInput {
                actor: admin.email,
                action: "set-account-password".to_string(),
                subject: account_email,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn update_account(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(account_id): AxumPath<Uuid>,
    Json(request): Json<UpdateAccountRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "accounts").await?;
    let dashboard_snapshot = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    let account = dashboard_snapshot
        .accounts
        .iter()
        .find(|account| account.id == account_id)
        .ok_or((StatusCode::NOT_FOUND, "account not found".to_string()))?;
    ensure_admin_can_manage_email(&admin, &account.email)?;

    let password_hash = match request.password.as_deref().map(str::trim) {
        Some("") | None => None,
        Some(password) if password.len() < 8 => {
            return Err((
                StatusCode::BAD_REQUEST,
                "account password must contain at least 8 characters".to_string(),
            ));
        }
        Some(password) => Some(hash_password(password).map_err(internal_error)?),
    };

    storage
        .update_account(
            UpdateAccount {
                account_id,
                display_name: request.display_name,
                quota_mb: request.quota_mb.max(256),
                status: request.status,
                gal_visibility: request
                    .gal_visibility
                    .unwrap_or_else(|| "tenant".to_string()),
                directory_kind: request
                    .directory_kind
                    .unwrap_or_else(|| "person".to_string()),
                password_hash,
            },
            AuditEntryInput {
                actor: admin.email,
                action: "update-account".to_string(),
                subject: account.email.clone(),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn create_mailbox(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateMailboxRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "accounts").await?;
    storage
        .create_mailbox(
            NewMailbox {
                account_id: request.account_id,
                display_name: request.display_name.clone(),
                role: request.role,
                retention_days: request.retention_days.max(1),
            },
            AuditEntryInput {
                actor: admin.email,
                action: "create-mailbox".to_string(),
                subject: format!("{} for {}", request.display_name, request.account_id),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn create_pst_transfer_job(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreatePstTransferJobRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "pst").await?;
    let dashboard_snapshot = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    let account_email = mailbox_account_email(&dashboard_snapshot, request.mailbox_id)
        .ok_or((StatusCode::NOT_FOUND, "mailbox not found".to_string()))?;
    ensure_admin_can_manage_email(&admin, &account_email)?;
    let direction = request.direction.trim().to_lowercase();
    let action = if direction == "export" {
        "request-pst-export"
    } else {
        "request-pst-import"
    };

    storage
        .create_pst_transfer_job(
            NewPstTransferJob {
                mailbox_id: request.mailbox_id,
                direction,
                server_path: request.server_path.clone(),
                requested_by: request.requested_by.clone(),
            },
            AuditEntryInput {
                actor: admin.email,
                action: action.to_string(),
                subject: format!("{} {}", request.mailbox_id, request.server_path),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn upload_pst_import(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(mailbox_id): AxumPath<Uuid>,
    mut multipart: Multipart,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "pst").await?;
    let dashboard_snapshot = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    let account_email = mailbox_account_email(&dashboard_snapshot, mailbox_id)
        .ok_or((StatusCode::NOT_FOUND, "mailbox not found".to_string()))?;
    ensure_admin_can_manage_email(&admin, &account_email)?;

    let mut requested_by = admin.email.clone();
    let mut uploaded_path: Option<PathBuf> = None;

    while let Some(mut field) = multipart.next_field().await.map_err(bad_request_error)? {
        let field_name = field.name().unwrap_or_default().to_string();
        if field_name == "requested_by" {
            requested_by = field.text().await.map_err(bad_request_error)?;
            continue;
        }

        if field_name != "file" {
            continue;
        }

        let file_name = field.file_name().unwrap_or("mailbox.pst").to_string();
        let declared_mime = field.content_type().map(ToString::to_string);

        let upload_dir = pst_import_dir();
        tokio::fs::create_dir_all(&upload_dir)
            .await
            .map_err(internal_error)?;
        let target_path = upload_dir.join(format!(
            "{}-{}",
            Uuid::new_v4(),
            sanitize_upload_filename(&file_name)
        ));
        let mut target_file = tokio::fs::File::create(&target_path)
            .await
            .map_err(internal_error)?;

        while let Some(chunk) = field.chunk().await.map_err(bad_request_error)? {
            target_file
                .write_all(&chunk)
                .await
                .map_err(internal_error)?;
        }
        target_file.flush().await.map_err(internal_error)?;
        validate_uploaded_pst_file(&target_path, &file_name, declared_mime.as_deref())
            .map_err(bad_request_error)?;
        uploaded_path = Some(target_path);
    }

    let uploaded_path = uploaded_path.ok_or((
        StatusCode::BAD_REQUEST,
        "missing PST upload file".to_string(),
    ))?;
    let server_path = uploaded_path.to_string_lossy().to_string();

    storage
        .create_pst_transfer_job(
            NewPstTransferJob {
                mailbox_id,
                direction: "import".to_string(),
                server_path: server_path.clone(),
                requested_by: requested_by.clone(),
            },
            AuditEntryInput {
                actor: admin.email,
                action: "upload-pst-import".to_string(),
                subject: format!("{requested_by} uploaded {server_path}"),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn create_domain(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateDomainRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "domains").await?;
    storage
        .create_domain(
            NewDomain {
                name: request.name.clone(),
                default_quota_mb: request.default_quota_mb.max(256),
                inbound_enabled: request.inbound_enabled,
                outbound_enabled: request.outbound_enabled,
                default_sieve_script: request.default_sieve_script.unwrap_or_default(),
            },
            AuditEntryInput {
                actor: admin.email,
                action: "create-domain".to_string(),
                subject: request.name,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn update_domain(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(domain_id): AxumPath<Uuid>,
    Json(request): Json<UpdateDomainRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "domains").await?;
    storage
        .update_domain(
            UpdateDomain {
                domain_id,
                default_quota_mb: request.default_quota_mb.max(256),
                inbound_enabled: request.inbound_enabled,
                outbound_enabled: request.outbound_enabled,
                default_sieve_script: request.default_sieve_script.unwrap_or_default(),
            },
            AuditEntryInput {
                actor: admin.email,
                action: "update-domain".to_string(),
                subject: domain_id.to_string(),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn create_alias(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateAliasRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "aliases").await?;
    ensure_admin_can_manage_email(&admin, &request.source)?;
    storage
        .create_alias(
            NewAlias {
                source: request.source.clone(),
                target: request.target,
                kind: request.kind,
            },
            AuditEntryInput {
                actor: admin.email,
                action: "create-alias".to_string(),
                subject: request.source,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn update_server_settings(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpdateServerSettingsRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "server").await?;
    let existing = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    storage
        .update_settings(
            DashboardUpdate {
                server_settings: ServerSettings {
                    primary_hostname: request.primary_hostname.clone(),
                    admin_bind_address: request.admin_bind_address,
                    smtp_bind_address: request.smtp_bind_address,
                    imap_bind_address: request.imap_bind_address,
                    jmap_bind_address: request.jmap_bind_address,
                    default_locale: request.default_locale,
                    max_message_size_mb: request.max_message_size_mb.max(8),
                    tls_mode: request.tls_mode,
                },
                security_settings: existing.security_settings,
                local_ai_settings: existing.local_ai_settings,
                antispam_settings: existing.antispam_settings,
            },
            AuditEntryInput {
                actor: admin.email,
                action: "update-server-settings".to_string(),
                subject: request.primary_hostname,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn update_security_settings(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpdateSecuritySettingsRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "security").await?;
    let existing = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    storage
        .update_settings(
            DashboardUpdate {
                server_settings: existing.server_settings,
                security_settings: SecuritySettings {
                    password_login_enabled: request.password_login_enabled,
                    mfa_required_for_admins: request.mfa_required_for_admins,
                    session_timeout_minutes: request.session_timeout_minutes.max(5),
                    audit_retention_days: request.audit_retention_days.max(30),
                    oidc_login_enabled: request.oidc_login_enabled,
                    oidc_provider_label: request.oidc_provider_label.trim().to_string(),
                    oidc_auto_link_by_email: request.oidc_auto_link_by_email,
                    oidc_issuer_url: request.oidc_issuer_url.trim().to_string(),
                    oidc_authorization_endpoint: request
                        .oidc_authorization_endpoint
                        .trim()
                        .to_string(),
                    oidc_token_endpoint: request.oidc_token_endpoint.trim().to_string(),
                    oidc_userinfo_endpoint: request.oidc_userinfo_endpoint.trim().to_string(),
                    oidc_client_id: request.oidc_client_id.trim().to_string(),
                    oidc_client_secret: request.oidc_client_secret.trim().to_string(),
                    oidc_scopes: request.oidc_scopes.trim().to_string(),
                    oidc_claim_email: request.oidc_claim_email.trim().to_string(),
                    oidc_claim_display_name: request.oidc_claim_display_name.trim().to_string(),
                    oidc_claim_subject: request.oidc_claim_subject.trim().to_string(),
                    mailbox_password_login_enabled: request
                        .mailbox_password_login_enabled
                        .unwrap_or(existing.security_settings.mailbox_password_login_enabled),
                    mailbox_oidc_login_enabled: request
                        .mailbox_oidc_login_enabled
                        .unwrap_or(existing.security_settings.mailbox_oidc_login_enabled),
                    mailbox_oidc_provider_label: request
                        .mailbox_oidc_provider_label
                        .unwrap_or(existing.security_settings.mailbox_oidc_provider_label)
                        .trim()
                        .to_string(),
                    mailbox_oidc_auto_link_by_email: request
                        .mailbox_oidc_auto_link_by_email
                        .unwrap_or(existing.security_settings.mailbox_oidc_auto_link_by_email),
                    mailbox_oidc_issuer_url: request
                        .mailbox_oidc_issuer_url
                        .unwrap_or(existing.security_settings.mailbox_oidc_issuer_url)
                        .trim()
                        .to_string(),
                    mailbox_oidc_authorization_endpoint: request
                        .mailbox_oidc_authorization_endpoint
                        .unwrap_or(
                            existing
                                .security_settings
                                .mailbox_oidc_authorization_endpoint,
                        )
                        .trim()
                        .to_string(),
                    mailbox_oidc_token_endpoint: request
                        .mailbox_oidc_token_endpoint
                        .unwrap_or(existing.security_settings.mailbox_oidc_token_endpoint)
                        .trim()
                        .to_string(),
                    mailbox_oidc_userinfo_endpoint: request
                        .mailbox_oidc_userinfo_endpoint
                        .unwrap_or(existing.security_settings.mailbox_oidc_userinfo_endpoint)
                        .trim()
                        .to_string(),
                    mailbox_oidc_client_id: request
                        .mailbox_oidc_client_id
                        .unwrap_or(existing.security_settings.mailbox_oidc_client_id)
                        .trim()
                        .to_string(),
                    mailbox_oidc_client_secret: request
                        .mailbox_oidc_client_secret
                        .unwrap_or(existing.security_settings.mailbox_oidc_client_secret)
                        .trim()
                        .to_string(),
                    mailbox_oidc_scopes: request
                        .mailbox_oidc_scopes
                        .unwrap_or(existing.security_settings.mailbox_oidc_scopes)
                        .trim()
                        .to_string(),
                    mailbox_oidc_claim_email: request
                        .mailbox_oidc_claim_email
                        .unwrap_or(existing.security_settings.mailbox_oidc_claim_email)
                        .trim()
                        .to_string(),
                    mailbox_oidc_claim_display_name: request
                        .mailbox_oidc_claim_display_name
                        .unwrap_or(existing.security_settings.mailbox_oidc_claim_display_name)
                        .trim()
                        .to_string(),
                    mailbox_oidc_claim_subject: request
                        .mailbox_oidc_claim_subject
                        .unwrap_or(existing.security_settings.mailbox_oidc_claim_subject)
                        .trim()
                        .to_string(),
                    mailbox_app_passwords_enabled: request
                        .mailbox_app_passwords_enabled
                        .unwrap_or(existing.security_settings.mailbox_app_passwords_enabled),
                },
                local_ai_settings: existing.local_ai_settings,
                antispam_settings: existing.antispam_settings,
            },
            AuditEntryInput {
                actor: admin.email,
                action: "update-security-settings".to_string(),
                subject: "admin policies".to_string(),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn update_local_ai_settings(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpdateLocalAiSettingsRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "ai").await?;
    let existing = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    storage
        .update_settings(
            DashboardUpdate {
                server_settings: existing.server_settings,
                security_settings: existing.security_settings,
                local_ai_settings: LocalAiSettings {
                    enabled: request.enabled,
                    provider: request.provider,
                    model: request.model.clone(),
                    offline_only: request.offline_only,
                    indexing_enabled: request.indexing_enabled,
                },
                antispam_settings: existing.antispam_settings,
            },
            AuditEntryInput {
                actor: admin.email,
                action: "update-local-ai-settings".to_string(),
                subject: request.model,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn update_antispam_settings(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpdateAntispamSettingsRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "antispam").await?;
    let existing = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    storage
        .update_settings(
            DashboardUpdate {
                server_settings: existing.server_settings,
                security_settings: existing.security_settings,
                local_ai_settings: existing.local_ai_settings,
                antispam_settings: lpe_storage::AntispamSettings {
                    content_filtering_enabled: request.content_filtering_enabled,
                    spam_engine: request.spam_engine.clone(),
                    quarantine_enabled: request.quarantine_enabled,
                    quarantine_retention_days: request.quarantine_retention_days.max(1),
                },
            },
            AuditEntryInput {
                actor: admin.email,
                action: "update-antispam-settings".to_string(),
                subject: request.spam_engine,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn create_server_administrator(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateServerAdministratorRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "admins").await?;
    let admin_email = request.email.clone();
    storage
        .create_server_administrator(
            NewServerAdministrator {
                domain_id: request.domain_id,
                email: request.email.clone(),
                display_name: request.display_name,
                role: request.role,
                rights_summary: request.rights_summary,
                permissions: request.permissions,
            },
            AuditEntryInput {
                actor: admin.email.clone(),
                action: "create-server-administrator".to_string(),
                subject: admin_email.clone(),
            },
        )
        .await
        .map_err(internal_error)?;

    if let Some(password) = request.password {
        if !password.trim().is_empty() {
            storage
                .upsert_admin_credential(
                    AdminCredentialInput {
                        email: admin_email.clone(),
                        password_hash: hash_password(&password).map_err(internal_error)?,
                    },
                    AuditEntryInput {
                        actor: admin.email,
                        action: "set-admin-password".to_string(),
                        subject: admin_email,
                    },
                )
                .await
                .map_err(internal_error)?;
        }
    }

    dashboard(State(storage), headers).await
}

async fn create_filter_rule(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateFilterRuleRequest>,
) -> ApiResult<AdminDashboard> {
    let admin = require_admin(&storage, &headers, "antispam").await?;
    storage
        .create_filter_rule(
            NewFilterRule {
                name: request.name.clone(),
                scope: request.scope,
                action: request.action,
                status: request.status,
            },
            AuditEntryInput {
                actor: admin.email,
                action: "create-antispam-rule".to_string(),
                subject: request.name,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage), headers).await
}

async fn search_email_trace(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<EmailTraceSearchRequest>,
) -> ApiResult<Vec<EmailTraceResult>> {
    require_admin(&storage, &headers, "audit").await?;
    Ok(Json(
        storage
            .search_email_trace(EmailTraceSearchInput {
                query: request.query,
            })
            .await
            .map_err(internal_error)?,
    ))
}

async fn run_pst_jobs(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<PstJobExecutionSummary> {
    require_admin(&storage, &headers, "pst").await?;
    Ok(Json(
        storage
            .process_pending_pst_jobs()
            .await
            .map_err(internal_error)?,
    ))
}

async fn submit_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SubmitMessageRequest>,
) -> ApiResult<SubmittedMessage> {
    let account = require_account(&storage, &headers).await?;
    let trace_id = observability::trace_id_from_headers(&headers);
    let subject_for_audit = request.subject.clone();
    let recipient_count = request.to.len()
        + request
            .cc
            .as_ref()
            .map(|entries| entries.len())
            .unwrap_or(0)
        + request
            .bcc
            .as_ref()
            .map(|entries| entries.len())
            .unwrap_or(0);
    let internet_message_id = request.internet_message_id.clone();
    ensure_client_message_owner(&account, &request)?;
    let submitted = storage
        .submit_message(
            map_submit_message_request(request),
            AuditEntryInput {
                actor: account.email,
                action: "submit-message".to_string(),
                subject: subject_for_audit,
            },
        )
        .await
        .map_err(internal_error)?;
    observability::record_mail_submission("api");
    info!(
        trace_id = %trace_id,
        account_id = %account.account_id,
        message_id = %submitted.message_id,
        internet_message_id = internet_message_id.as_deref().unwrap_or(""),
        recipient_count,
        "mail submission accepted"
    );

    Ok(Json(submitted))
}

async fn save_draft_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SubmitMessageRequest>,
) -> ApiResult<SavedDraftMessage> {
    let account = require_account(&storage, &headers).await?;
    let subject_for_audit = request.subject.clone();
    ensure_client_message_owner(&account, &request)?;
    let draft = storage
        .save_draft_message(
            map_submit_message_request(request),
            AuditEntryInput {
                actor: account.email,
                action: "save-draft-message".to_string(),
                subject: subject_for_audit,
            },
        )
        .await
        .map_err(internal_error)?;

    Ok(Json(draft))
}

async fn delete_draft_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(message_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_draft_message(
            account.account_id,
            message_id,
            AuditEntryInput {
                actor: account.email,
                action: "delete-draft-message".to_string(),
                subject: message_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn upsert_client_contact(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertClientContactRequest>,
) -> ApiResult<ClientContact> {
    let account = require_account(&storage, &headers).await?;
    let input = UpsertClientContactInput {
        id: request.id,
        account_id: account.account_id,
        name: request.name,
        role: request.role,
        email: request.email,
        phone: request.phone,
        team: request.team,
        notes: request.notes,
    };
    let contact = if let Some(contact_id) = request.id {
        storage
            .update_accessible_contact(account.account_id, contact_id, input)
            .await
            .map_err(bad_request_error)?
    } else {
        storage
            .create_accessible_contact(account.account_id, request.collection_id.as_deref(), input)
            .await
            .map_err(bad_request_error)?
    };
    Ok(Json(ClientContact {
        id: contact.id,
        name: contact.name,
        role: contact.role,
        email: contact.email,
        phone: contact.phone,
        team: contact.team,
        notes: contact.notes,
    }))
}

async fn upsert_client_event(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertClientEventRequest>,
) -> ApiResult<ClientEvent> {
    let account = require_account(&storage, &headers).await?;
    let input = UpsertClientEventInput {
        id: request.id,
        account_id: account.account_id,
        date: request.date,
        time: request.time,
        time_zone: request.time_zone,
        duration_minutes: request.duration_minutes,
        recurrence_rule: request.recurrence_rule,
        title: request.title,
        location: request.location,
        attendees: request.attendees,
        attendees_json: request.attendees_json,
        notes: request.notes,
    };
    let event = if let Some(event_id) = request.id {
        storage
            .update_accessible_event(account.account_id, event_id, input)
            .await
            .map_err(bad_request_error)?
    } else {
        storage
            .create_accessible_event(account.account_id, request.collection_id.as_deref(), input)
            .await
            .map_err(bad_request_error)?
    };
    Ok(Json(ClientEvent {
        id: event.id,
        date: event.date,
        time: event.time,
        time_zone: event.time_zone,
        duration_minutes: event.duration_minutes,
        recurrence_rule: event.recurrence_rule,
        title: event.title,
        location: event.location,
        attendees: event.attendees,
        attendees_json: event.attendees_json,
        notes: event.notes,
    }))
}

async fn list_collaboration_overview(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<CollaborationOverviewResponse> {
    let account = require_account(&storage, &headers).await?;
    let incoming_contact_collections = storage
        .fetch_accessible_contact_collections(account.account_id)
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|collection| !collection.is_owned)
        .collect();
    let incoming_calendar_collections = storage
        .fetch_accessible_calendar_collections(account.account_id)
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|collection| !collection.is_owned)
        .collect();

    Ok(Json(CollaborationOverviewResponse {
        outgoing_contacts: storage
            .fetch_outgoing_collaboration_grants(
                account.account_id,
                CollaborationResourceKind::Contacts,
            )
            .await
            .map_err(internal_error)?,
        outgoing_calendars: storage
            .fetch_outgoing_collaboration_grants(
                account.account_id,
                CollaborationResourceKind::Calendar,
            )
            .await
            .map_err(internal_error)?,
        incoming_contact_collections,
        incoming_calendar_collections,
    }))
}

async fn upsert_collaboration_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertCollaborationGrantRequest>,
) -> ApiResult<lpe_storage::CollaborationGrant> {
    let account = require_account(&storage, &headers).await?;
    let kind = parse_collaboration_kind(&request.kind).map_err(bad_request_error)?;
    Ok(Json(
        storage
            .upsert_collaboration_grant(
                CollaborationGrantInput {
                    kind,
                    owner_account_id: account.account_id,
                    grantee_email: request.grantee_email.clone(),
                    may_read: request.may_read,
                    may_write: request.may_write,
                    may_delete: request.may_delete,
                    may_share: request.may_share,
                },
                AuditEntryInput {
                    actor: account.email.clone(),
                    action: format!("collaboration-share-upsert:{}", kind.as_str()),
                    subject: request.grantee_email,
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

async fn delete_collaboration_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((kind, grantee_account_id)): AxumPath<(String, Uuid)>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    let kind = parse_collaboration_kind(&kind).map_err(bad_request_error)?;
    storage
        .delete_collaboration_grant(
            account.account_id,
            kind,
            grantee_account_id,
            AuditEntryInput {
                actor: account.email,
                action: format!("collaboration-share-delete:{}", kind.as_str()),
                subject: grantee_account_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn get_mailbox_delegation(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<MailboxDelegationResponse> {
    let account = require_account(&storage, &headers).await?;
    let incoming_mailboxes = storage
        .fetch_accessible_mailbox_accounts(account.account_id)
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|entry| !entry.is_owned)
        .collect();
    let overview = lpe_storage::MailboxDelegationOverview {
        outgoing_mailboxes: storage
            .fetch_outgoing_mailbox_delegation_grants(account.account_id)
            .await
            .map_err(internal_error)?,
        incoming_mailboxes,
        outgoing_sender_rights: storage
            .fetch_outgoing_sender_delegation_grants(account.account_id)
            .await
            .map_err(internal_error)?,
    };
    Ok(Json(MailboxDelegationResponse { overview }))
}

async fn upsert_mailbox_delegation_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertMailboxDelegationGrantRequest>,
) -> ApiResult<lpe_storage::MailboxDelegationGrant> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .upsert_mailbox_delegation_grant(
                MailboxDelegationGrantInput {
                    owner_account_id: account.account_id,
                    grantee_email: request.grantee_email.clone(),
                },
                AuditEntryInput {
                    actor: account.email.clone(),
                    action: "mailbox-delegation-upsert".to_string(),
                    subject: request.grantee_email,
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

async fn delete_mailbox_delegation_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(grantee_account_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_mailbox_delegation_grant(
            account.account_id,
            grantee_account_id,
            AuditEntryInput {
                actor: account.email,
                action: "mailbox-delegation-delete".to_string(),
                subject: grantee_account_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn upsert_sender_delegation_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertSenderDelegationGrantRequest>,
) -> ApiResult<SenderDelegationGrant> {
    let account = require_account(&storage, &headers).await?;
    let sender_right =
        parse_sender_delegation_right(&request.sender_right).map_err(bad_request_error)?;
    Ok(Json(
        storage
            .upsert_sender_delegation_grant(
                SenderDelegationGrantInput {
                    owner_account_id: account.account_id,
                    grantee_email: request.grantee_email.clone(),
                    sender_right,
                },
                AuditEntryInput {
                    actor: account.email.clone(),
                    action: format!("sender-delegation-upsert:{}", sender_right.as_str()),
                    subject: request.grantee_email,
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

async fn delete_sender_delegation_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((sender_right, grantee_account_id)): AxumPath<(String, Uuid)>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    let sender_right = parse_sender_delegation_right(&sender_right).map_err(bad_request_error)?;
    storage
        .delete_sender_delegation_grant(
            account.account_id,
            grantee_account_id,
            sender_right,
            AuditEntryInput {
                actor: account.email,
                action: format!("sender-delegation-delete:{}", sender_right.as_str()),
                subject: grantee_account_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn get_sieve_overview(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<SieveOverviewResponse> {
    let account = require_account(&storage, &headers).await?;
    let scripts = storage
        .list_sieve_scripts(account.account_id)
        .await
        .map_err(internal_error)?;
    let active_script = storage
        .fetch_active_sieve_script(account.account_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(SieveOverviewResponse {
        scripts,
        active_script,
    }))
}

async fn get_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<SieveScriptDocument> {
    let account = require_account(&storage, &headers).await?;
    let script = storage
        .get_sieve_script(account.account_id, &name)
        .await
        .map_err(bad_request_error)?
        .ok_or((StatusCode::NOT_FOUND, "sieve script not found".to_string()))?;
    Ok(Json(script))
}

async fn put_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertSieveScriptRequest>,
) -> ApiResult<SieveScriptDocument> {
    let account = require_account(&storage, &headers).await?;
    let subject = request.name.clone();
    Ok(Json(
        storage
            .put_sieve_script(
                account.account_id,
                &request.name,
                &request.content,
                request.activate,
                AuditEntryInput {
                    actor: account.email,
                    action: "sieve-script-upsert".to_string(),
                    subject,
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

async fn rename_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<RenameSieveScriptRequest>,
) -> ApiResult<lpe_storage::SieveScriptSummary> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .rename_sieve_script(
                account.account_id,
                &request.old_name,
                &request.new_name,
                AuditEntryInput {
                    actor: account.email,
                    action: "sieve-script-rename".to_string(),
                    subject: format!("{} -> {}", request.old_name, request.new_name),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

async fn set_active_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SetActiveSieveScriptRequest>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    let subject = request.name.clone().unwrap_or_else(|| "none".to_string());
    storage
        .set_active_sieve_script(
            account.account_id,
            request.name.as_deref(),
            AuditEntryInput {
                actor: account.email,
                action: "sieve-script-activate".to_string(),
                subject,
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn delete_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    let subject = name.clone();
    storage
        .delete_sieve_script(
            account.account_id,
            &name,
            AuditEntryInput {
                actor: account.email,
                action: "sieve-script-delete".to_string(),
                subject,
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn mail_flow(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<MailFlowResponse> {
    require_admin(&storage, &headers, "operations").await?;
    let items = storage
        .fetch_mail_flow_entries()
        .await
        .map_err(internal_error)?;
    Ok(Json(MailFlowResponse { items }))
}

async fn list_client_tasks(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<ClientTask>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_client_tasks(account.account_id)
            .await
            .map_err(internal_error)?,
    ))
}

async fn get_client_task(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<Uuid>,
) -> ApiResult<ClientTask> {
    let account = require_account(&storage, &headers).await?;
    let mut tasks = storage
        .fetch_client_tasks_by_ids(account.account_id, &[task_id])
        .await
        .map_err(internal_error)?;
    let task = tasks
        .pop()
        .ok_or((StatusCode::NOT_FOUND, "task not found".to_string()))?;
    Ok(Json(task))
}

async fn upsert_client_task(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertClientTaskRequest>,
) -> ApiResult<ClientTask> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .upsert_client_task(UpsertClientTaskInput {
                id: request.id,
                account_id: account.account_id,
                task_list_id: request.task_list_id,
                title: request.title,
                description: request.description,
                status: request.status,
                due_at: request.due_at,
                completed_at: request.completed_at,
                sort_order: request.sort_order.unwrap_or(0),
            })
            .await
            .map_err(bad_request_error)?,
    ))
}

async fn delete_client_task(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(task_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_client_task(account.account_id, task_id)
        .await
        .map_err(bad_request_error)?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

async fn deliver_inbound_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<InboundDeliveryRequest>,
) -> ApiResult<InboundDeliveryResponse> {
    require_integration(&headers)?;
    if !ha_allows_active_work().map_err(internal_error)? {
        let role = ha_current_role()
            .map_err(internal_error)?
            .unwrap_or_else(|| "standby".to_string());
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("node role {role} does not accept LPE-CT inbound deliveries"),
        ));
    }
    let trace_id = request.trace_id.clone();
    let internet_message_id = request.internet_message_id.clone();
    let recipient_count = request.rcpt_to.len();
    let response = storage
        .deliver_inbound_message(request)
        .await
        .map_err(bad_request_error)?;
    observability::record_inbound_delivery(response.status.as_str());
    info!(
        trace_id = %trace_id,
        status = response.status.as_str(),
        accepted_recipients = response.accepted_recipients.len(),
        rejected_recipients = response.rejected_recipients.len(),
        stored_messages = response.stored_message_ids.len(),
        recipient_count,
        internet_message_id = internet_message_id.as_deref().unwrap_or(""),
        "inbound delivery processed"
    );
    Ok(Json(response))
}

async fn authenticate_smtp_submission(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SmtpSubmissionAuthRequest>,
) -> ApiResult<SmtpSubmissionAuthResponse> {
    require_integration(&headers)?;
    let principal = authenticate_plain_credentials(
        &storage,
        None,
        &request.username,
        &request.password,
        "smtp",
    )
    .await
    .map_err(|error| (StatusCode::UNAUTHORIZED, error.to_string()))?;
    Ok(Json(SmtpSubmissionAuthResponse {
        tenant_id: principal.tenant_id,
        account_id: principal.account_id,
        email: principal.email,
        display_name: principal.display_name,
    }))
}

async fn accept_smtp_submission(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SmtpSubmissionRequest>,
) -> ApiResult<SmtpSubmissionResponse> {
    require_integration(&headers)?;
    if !ha_allows_active_work().map_err(internal_error)? {
        let role = ha_current_role()
            .map_err(internal_error)?
            .unwrap_or_else(|| "standby".to_string());
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("node role {role} does not accept LPE-CT smtp submissions"),
        ));
    }

    let principal = AccountPrincipal {
        tenant_id: String::new(),
        account_id: request.account_id,
        email: request.account_email.trim().to_lowercase(),
        display_name: request.account_display_name.clone(),
    };
    let submit_input = build_smtp_submission_input(&storage, &principal, &request)
        .await
        .map_err(bad_request_error)?;
    let submitted = storage
        .submit_message(
            submit_input,
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "smtp-submission".to_string(),
                subject: "client smtp submission".to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;
    observability::record_mail_submission("smtp");
    info!(
        trace_id = %request.trace_id,
        account_id = %principal.account_id,
        message_id = %submitted.message_id,
        outbound_queue_id = %submitted.outbound_queue_id,
        peer = %request.peer,
        helo = %request.helo,
        recipient_count = request.rcpt_to.len(),
        "smtp submission accepted from lpe-ct"
    );

    Ok(Json(SmtpSubmissionResponse {
        trace_id: request.trace_id,
        message_id: submitted.message_id,
        outbound_queue_id: submitted.outbound_queue_id,
        delivery_status: submitted.delivery_status,
    }))
}

async fn build_smtp_submission_input(
    storage: &Storage,
    principal: &AccountPrincipal,
    request: &SmtpSubmissionRequest,
) -> anyhow::Result<SubmitMessageInput> {
    let parsed = lpe_storage::mail::parse_rfc822_message(&request.raw_message)?;
    validate_smtp_submission_attachments(&request.raw_message)?;
    let envelope_from = request
        .mail_from
        .trim()
        .trim_matches(['<', '>'])
        .to_lowercase();
    if envelope_from.is_empty() {
        anyhow::bail!("smtp submission requires MAIL FROM");
    }
    if request.rcpt_to.is_empty() {
        anyhow::bail!("smtp submission requires at least one RCPT TO recipient");
    }

    let from = parsed
        .from
        .as_ref()
        .map(|address| address.email.trim().to_lowercase())
        .unwrap_or_else(|| principal.email.clone());
    let owner = if from == principal.email {
        SubmissionAccountIdentity {
            account_id: principal.account_id,
            email: principal.email.clone(),
            display_name: principal.display_name.clone(),
        }
    } else {
        storage
            .find_submission_account_by_email_in_same_tenant(principal.account_id, &from)
            .await?
            .ok_or_else(|| anyhow::anyhow!("delegated From address is not a mailbox in the same tenant"))?
    };
    if envelope_from != principal.email && envelope_from != owner.email {
        anyhow::bail!(
            "smtp submission MAIL FROM must match the authenticated account or delegated mailbox"
        );
    }

    let visible_to = parsed
        .to
        .iter()
        .cloned()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.email,
            display_name: recipient.display_name,
        })
        .collect::<Vec<_>>();
    let visible_cc = parsed
        .cc
        .iter()
        .cloned()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.email,
            display_name: recipient.display_name,
        })
        .collect::<Vec<_>>();
    let bcc = merge_smtp_bcc_recipients(
        &request.raw_message,
        &request.rcpt_to,
        &visible_to,
        &visible_cc,
    );
    let sender =
        parse_smtp_submission_sender(&request.raw_message, &from, &principal.email, &owner.email)?;

    Ok(build_smtp_submission_input_for_owner(
        principal, &owner, request, parsed, visible_to, visible_cc, bcc, sender,
    ))
}

fn build_smtp_submission_input_for_owner(
    principal: &AccountPrincipal,
    owner: &SubmissionAccountIdentity,
    request: &SmtpSubmissionRequest,
    parsed: lpe_storage::mail::ParsedRfc822Message,
    to: Vec<SubmittedRecipientInput>,
    cc: Vec<SubmittedRecipientInput>,
    bcc: Vec<SubmittedRecipientInput>,
    sender: Option<SubmittedRecipientInput>,
) -> SubmitMessageInput {
    let from_display = parsed
        .from
        .as_ref()
        .and_then(|address| address.display_name.clone())
        .or_else(|| Some(owner.display_name.clone()));

    SubmitMessageInput {
        draft_message_id: None,
        account_id: owner.account_id,
        submitted_by_account_id: principal.account_id,
        source: "smtp-submission".to_string(),
        from_display,
        from_address: owner.email.clone(),
        sender_display: sender
            .as_ref()
            .and_then(|address| address.display_name.clone())
            .or_else(|| sender.as_ref().map(|_| principal.display_name.clone())),
        sender_address: sender.map(|address| address.address),
        to,
        cc,
        bcc,
        subject: parsed.subject,
        body_text: parsed.body_text,
        body_html_sanitized: parsed.body_html_sanitized,
        internet_message_id: parsed.message_id,
        mime_blob_ref: Some(format!("smtp-submission-mime:{}", Uuid::new_v4())),
        size_octets: request.raw_message.len() as i64,
        unread: Some(false),
        flagged: Some(false),
        attachments: parsed.attachments,
    }
}

fn parse_smtp_submission_sender(
    raw_message: &[u8],
    from_address: &str,
    principal_email: &str,
    owner_email: &str,
) -> anyhow::Result<Option<SubmittedRecipientInput>> {
    let sender = lpe_storage::mail::parse_header_recipients(raw_message, "sender")
        .into_iter()
        .next();
    let Some(sender) = sender else {
        return Ok(None);
    };
    let normalized_sender = sender.address.trim().to_lowercase();
    if normalized_sender.is_empty()
        || normalized_sender == from_address
        || normalized_sender == owner_email
    {
        return Ok(None);
    }
    if normalized_sender != principal_email {
        anyhow::bail!("authenticated account cannot submit a different Sender address");
    }
    Ok(Some(SubmittedRecipientInput {
        address: normalized_sender,
        display_name: sender.display_name,
    }))
}

fn merge_smtp_bcc_recipients(
    raw_message: &[u8],
    envelope_recipients: &[String],
    to: &[SubmittedRecipientInput],
    cc: &[SubmittedRecipientInput],
) -> Vec<SubmittedRecipientInput> {
    let mut visible = to
        .iter()
        .chain(cc.iter())
        .map(|recipient| recipient.address.trim().to_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    let mut merged = Vec::new();
    let mut seen = std::collections::BTreeSet::new();

    for recipient in lpe_storage::mail::parse_header_recipients(raw_message, "bcc") {
        let normalized = recipient.address.trim().to_lowercase();
        if !normalized.is_empty() && seen.insert(normalized.clone()) {
            visible.insert(normalized);
            merged.push(recipient);
        }
    }

    for recipient in envelope_recipients {
        let normalized = recipient.trim().trim_matches(['<', '>']).to_lowercase();
        if !normalized.is_empty()
            && !visible.contains(&normalized)
            && seen.insert(normalized.clone())
        {
            merged.push(SubmittedRecipientInput {
                address: normalized,
                display_name: None,
            });
        }
    }

    merged
}

fn validate_smtp_submission_attachments(raw_message: &[u8]) -> anyhow::Result<()> {
    let validator = Validator::from_env();
    for attachment in collect_mime_attachment_parts(raw_message)? {
        let outcome = validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::SmtpClientSubmission,
                declared_mime: attachment.declared_mime.clone(),
                filename: attachment.filename.clone(),
                expected_kind: ExpectedKind::Any,
            },
            &attachment.bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            anyhow::bail!(
                "smtp submission blocked by Magika validation for {:?}: {}",
                attachment.filename,
                outcome.reason
            );
        }
    }
    Ok(())
}

fn ensure_client_message_owner(
    account: &AuthenticatedAccount,
    request: &SubmitMessageRequest,
) -> std::result::Result<(), (StatusCode, String)> {
    if account.account_id == request.account_id
        && account.email.to_lowercase() == request.from_address.trim().to_lowercase()
    {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "authenticated account cannot submit this message".to_string(),
        ))
    }
}

fn map_submit_message_request(request: SubmitMessageRequest) -> SubmitMessageInput {
    SubmitMessageInput {
        draft_message_id: request.draft_message_id,
        account_id: request.account_id,
        submitted_by_account_id: request.account_id,
        source: request.source.unwrap_or_else(|| "jmap".to_string()),
        from_display: request.from_display,
        from_address: request.from_address,
        sender_display: None,
        sender_address: None,
        to: map_recipients(request.to),
        cc: map_recipients(request.cc.unwrap_or_default()),
        bcc: map_recipients(request.bcc.unwrap_or_default()),
        subject: request.subject,
        body_text: request.body_text,
        body_html_sanitized: request.body_html_sanitized,
        internet_message_id: request.internet_message_id,
        mime_blob_ref: request.mime_blob_ref,
        size_octets: request.size_octets.unwrap_or(0),
        unread: None,
        flagged: None,
        attachments: Vec::new(),
    }
}

fn map_recipients(input: Vec<SubmitRecipientRequest>) -> Vec<SubmittedRecipientInput> {
    input
        .into_iter()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.address,
            display_name: recipient.display_name,
        })
        .collect()
}

fn internal_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn bad_request_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, error.to_string())
}

fn parse_collaboration_kind(value: &str) -> Result<CollaborationResourceKind, String> {
    match value.trim().to_lowercase().as_str() {
        "contacts" | "contact" => Ok(CollaborationResourceKind::Contacts),
        "calendar" | "calendars" => Ok(CollaborationResourceKind::Calendar),
        _ => Err("unsupported collaboration kind".to_string()),
    }
}

fn parse_sender_delegation_right(value: &str) -> Result<SenderDelegationRight, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "send_as" | "send-as" => Ok(SenderDelegationRight::SendAs),
        "send_on_behalf" | "send-on-behalf" => Ok(SenderDelegationRight::SendOnBehalf),
        _ => Err("unsupported sender delegation right".to_string()),
    }
}

fn require_integration(headers: &HeaderMap) -> std::result::Result<(), (StatusCode, String)> {
    let provided = headers
        .get("x-lpe-integration-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            observability::record_security_event("integration_auth_failure");
            (
                StatusCode::UNAUTHORIZED,
                "missing integration key".to_string(),
            )
        })?;
    let expected = integration_shared_secret().map_err(internal_error)?;
    if provided == expected {
        Ok(())
    } else {
        observability::record_security_event("integration_auth_failure");
        Err((
            StatusCode::UNAUTHORIZED,
            "invalid integration key".to_string(),
        ))
    }
}

async fn require_admin(
    storage: &Storage,
    headers: &HeaderMap,
    right: &str,
) -> std::result::Result<AuthenticatedAdmin, (StatusCode, String)> {
    let token = bearer_token(headers)
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".to_string()))?;
    let admin = storage
        .fetch_admin_session(&token)
        .await
        .map_err(internal_error)?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "invalid or expired session".to_string(),
        ))?;

    if admin_has_right(&admin, right) {
        Ok(admin)
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "insufficient admin rights".to_string(),
        ))
    }
}

async fn require_account(
    storage: &Storage,
    headers: &HeaderMap,
) -> std::result::Result<AuthenticatedAccount, (StatusCode, String)> {
    let token = bearer_token(headers)
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer token".to_string()))?;
    storage
        .fetch_account_session(&token)
        .await
        .map_err(internal_error)?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "invalid or expired session".to_string(),
        ))
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(ToString::to_string)
}

fn admin_has_right(admin: &AuthenticatedAdmin, right: &str) -> bool {
    admin
        .permissions
        .iter()
        .any(|entry| entry == right || entry == "*")
}

fn public_origin(headers: &HeaderMap) -> String {
    let scheme = forwarded_header(headers, "x-forwarded-proto")
        .or_else(|| env::var("LPE_PUBLIC_SCHEME").ok())
        .unwrap_or_else(|| "http".to_string());
    let host = forwarded_header(headers, "x-forwarded-host")
        .or_else(|| forwarded_header(headers, "host"))
        .or_else(|| env::var("LPE_PUBLIC_HOSTNAME").ok())
        .unwrap_or_else(|| "localhost".to_string());
    format!("{}://{}", scheme.trim(), host.trim())
}

fn forwarded_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn ensure_admin_can_manage_email(
    admin: &AuthenticatedAdmin,
    email: &str,
) -> std::result::Result<(), (StatusCode, String)> {
    if admin.role == "server-admin" || admin.role == "super-admin" || admin.domain_id.is_none() {
        return Ok(());
    }

    let suffix = format!("@{}", admin.domain_name.to_lowercase());
    if email.trim().to_lowercase().ends_with(&suffix) {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "domain admin cannot manage this domain".to_string(),
        ))
    }
}

fn mailbox_account_email(dashboard: &AdminDashboard, mailbox_id: Uuid) -> Option<String> {
    dashboard
        .accounts
        .iter()
        .find(|account| {
            account
                .mailboxes
                .iter()
                .any(|mailbox| mailbox.id == mailbox_id)
        })
        .map(|account| account.email.clone())
}

fn pst_import_dir() -> PathBuf {
    env::var("LPE_PST_IMPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/lib/lpe/imports"))
}

fn pst_upload_max_bytes() -> usize {
    env::var("LPE_PST_UPLOAD_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20 * 1024 * 1024 * 1024)
}

fn lpe_ct_base_url() -> String {
    env::var("LPE_CT_API_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8380".to_string())
        .trim_end_matches('/')
        .to_string()
}

fn ha_role_file() -> Option<PathBuf> {
    env::var("LPE_HA_ROLE_FILE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn read_ha_role() -> anyhow::Result<Option<String>> {
    let Some(path) = ha_role_file() else {
        return Ok(None);
    };

    let role = std::fs::read_to_string(&path)
        .map_err(|error| anyhow::anyhow!("unable to read {}: {error}", path.display()))?;
    let normalized = role.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        anyhow::bail!("HA role file {} is empty", path.display());
    }
    if !matches!(
        normalized.as_str(),
        "active" | "standby" | "drain" | "maintenance"
    ) {
        anyhow::bail!(
            "HA role file {} contains unsupported role {}",
            path.display(),
            normalized
        );
    }

    Ok(Some(normalized))
}

fn ha_activation_check() -> ReadinessCheck {
    match read_ha_role() {
        Ok(None) => readiness_ok(
            "ha-role",
            true,
            "HA role gating disabled; node follows default single-node readiness",
        ),
        Ok(Some(role)) if role == "active" => {
            readiness_ok("ha-role", true, "node is marked active for HA traffic")
        }
        Ok(Some(role)) => readiness_failed(
            "ha-role",
            true,
            format!("node is marked {role} and must not receive active traffic"),
        ),
        Err(error) => readiness_failed("ha-role", true, error.to_string()),
    }
}

fn readiness_ok(name: &str, critical: bool, detail: impl Into<String>) -> ReadinessCheck {
    ReadinessCheck {
        name: name.to_string(),
        status: "ok".to_string(),
        critical,
        detail: detail.into(),
    }
}

fn readiness_warn(name: &str, detail: impl Into<String>) -> ReadinessCheck {
    ReadinessCheck {
        name: name.to_string(),
        status: "warn".to_string(),
        critical: false,
        detail: detail.into(),
    }
}

fn readiness_failed(name: &str, critical: bool, detail: impl Into<String>) -> ReadinessCheck {
    ReadinessCheck {
        name: name.to_string(),
        status: "failed".to_string(),
        critical,
        detail: detail.into(),
    }
}

fn build_readiness_response(service: &str, checks: Vec<ReadinessCheck>) -> ReadinessResponse {
    let has_critical_failure = checks
        .iter()
        .any(|check| check.critical && check.status == "failed");
    let warnings = checks.iter().filter(|check| check.status == "warn").count() as u32;

    ReadinessResponse {
        service: service.to_string(),
        status: if has_critical_failure {
            "failed".to_string()
        } else {
            "ready".to_string()
        },
        warnings,
        checks,
    }
}

async fn check_optional_http_dependency(
    name: &str,
    url: &str,
    ok_detail: &str,
    warn_detail: &str,
) -> ReadinessCheck {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(1_500))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return readiness_warn(
                name,
                format!("unable to initialize HTTP client for {url}: {error}"),
            );
        }
    };

    match client.get(url).send().await {
        Ok(response) if response.status().is_success() => readiness_ok(name, false, ok_detail),
        Ok(response) => readiness_warn(
            name,
            format!("{warn_detail} ({url} returned HTTP {})", response.status()),
        ),
        Err(error) => readiness_warn(name, format!("{warn_detail} ({url}: {error})")),
    }
}

fn validate_uploaded_pst_file(
    path: &Path,
    file_name: &str,
    declared_mime: Option<&str>,
) -> anyhow::Result<()> {
    validate_uploaded_pst_file_with_validator(
        &Validator::from_env(),
        path,
        file_name,
        declared_mime,
    )
}

fn validate_uploaded_pst_file_with_validator<D: Detector>(
    validator: &Validator<D>,
    path: &Path,
    file_name: &str,
    declared_mime: Option<&str>,
) -> anyhow::Result<()> {
    let request = ValidationRequest {
        ingress_context: IngressContext::PstUpload,
        declared_mime: declared_mime.map(ToString::to_string),
        filename: Some(file_name.to_string()),
        expected_kind: ExpectedKind::Pst,
    };
    let outcome = validator.validate_path(request.clone(), path)?;
    if outcome.policy_decision != PolicyDecision::Accept {
        let _ = std::fs::remove_file(path);
        return Err(anyhow::anyhow!(
            "PST upload blocked by Magika validation: {}",
            outcome.reason
        ));
    }

    write_validation_record(path, &request, &outcome, std::fs::metadata(path)?.len())?;
    Ok(())
}

fn sanitize_upload_filename(file_name: &str) -> String {
    let basename = Path::new(file_name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("mailbox.pst");
    let sanitized: String = basename
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() || matches!(value, '.' | '-' | '_') {
                value
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() {
        "mailbox.pst".to_string()
    } else {
        sanitized
    }
}

pub fn bootstrap_admin_request_from_env() -> anyhow::Result<BootstrapAdminRequest> {
    let email = required_env("LPE_BOOTSTRAP_ADMIN_EMAIL")?;
    let display_name = env::var("LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME")
        .unwrap_or_else(|_| "Bootstrap Administrator".to_string())
        .trim()
        .to_string();
    let password = required_env("LPE_BOOTSTRAP_ADMIN_PASSWORD")?;

    validate_bootstrap_admin_request(&email, &display_name, &password)?;

    Ok(BootstrapAdminRequest {
        email,
        display_name,
        password,
    })
}

pub fn bootstrap_admin_request_from_env_or_defaults() -> anyhow::Result<BootstrapAdminRequest> {
    let email = required_env("LPE_BOOTSTRAP_ADMIN_EMAIL")?;
    let display_name = env::var("LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME")
        .unwrap_or_else(|_| "Bootstrap Administrator".to_string())
        .trim()
        .to_string();
    let password = required_env("LPE_BOOTSTRAP_ADMIN_PASSWORD")?;

    validate_bootstrap_admin_request(&email, &display_name, &password)?;

    Ok(BootstrapAdminRequest {
        email,
        display_name,
        password,
    })
}

pub async fn bootstrap_admin(
    storage: &Storage,
    request: BootstrapAdminRequest,
) -> anyhow::Result<BootstrapAdminResponse> {
    validate_bootstrap_admin_request(&request.email, &request.display_name, &request.password)?;

    if storage.has_admin_bootstrap_state().await? {
        anyhow::bail!("bootstrap administrator already exists");
    }

    let email = request.email.trim().to_lowercase();
    let display_name = request.display_name.trim().to_string();
    storage
        .create_server_administrator(
            NewServerAdministrator {
                domain_id: None,
                email: email.clone(),
                display_name: display_name.clone(),
                role: "server-admin".to_string(),
                rights_summary:
                    "server, domains, accounts, aliases, admins, policies, security, ai, antispam, pst, audit, mail"
                        .to_string(),
                permissions: vec!["*".to_string()],
            },
            AuditEntryInput {
                actor: "bootstrap-cli".to_string(),
                action: "create-bootstrap-admin".to_string(),
                subject: email.clone(),
            },
        )
        .await?;

    storage
        .upsert_admin_credential(
            AdminCredentialInput {
                email: email.clone(),
                password_hash: hash_password(&request.password)?,
            },
            AuditEntryInput {
                actor: "bootstrap-cli".to_string(),
                action: "set-bootstrap-password".to_string(),
                subject: email.clone(),
            },
        )
        .await?;

    Ok(BootstrapAdminResponse {
        email,
        display_name,
    })
}

pub fn integration_shared_secret() -> anyhow::Result<String> {
    let secret = required_env("LPE_INTEGRATION_SHARED_SECRET")?;
    validate_shared_secret("LPE_INTEGRATION_SHARED_SECRET", &secret)?;
    Ok(secret)
}

pub fn init_observability(service_name: &str) {
    observability::init_tracing(service_name);
}

pub fn ha_allows_active_work() -> anyhow::Result<bool> {
    match read_ha_role()? {
        None => Ok(true),
        Some(role) => Ok(role == "active"),
    }
}

pub fn ha_current_role() -> anyhow::Result<Option<String>> {
    read_ha_role()
}

pub fn observe_outbound_worker_poll(batch_size: usize) {
    observability::record_outbound_worker_poll(batch_size);
}

pub fn observe_outbound_worker_dispatch(status: &str) {
    observability::record_outbound_dispatch(status);
}

fn required_env(name: &str) -> anyhow::Result<String> {
    let value = env::var(name)
        .map_err(|_| anyhow::anyhow!("{name} must be set"))?
        .trim()
        .to_string();
    if value.is_empty() {
        anyhow::bail!("{name} must not be empty");
    }
    Ok(value)
}

fn validate_bootstrap_admin_request(
    email: &str,
    display_name: &str,
    password: &str,
) -> anyhow::Result<()> {
    if !email.contains('@') {
        anyhow::bail!("bootstrap admin email must contain '@'");
    }
    if display_name.trim().is_empty() {
        anyhow::bail!("bootstrap admin display name must not be empty");
    }
    validate_admin_password(password)?;
    Ok(())
}

fn validate_admin_password(password: &str) -> anyhow::Result<()> {
    let trimmed = password.trim();
    if trimmed.len() < MIN_ADMIN_PASSWORD_LEN {
        anyhow::bail!(
            "bootstrap admin password must contain at least {MIN_ADMIN_PASSWORD_LEN} characters"
        );
    }
    if is_known_weak_secret(trimmed) {
        anyhow::bail!("bootstrap admin password uses a forbidden weak placeholder value");
    }
    Ok(())
}

fn validate_shared_secret(name: &str, secret: &str) -> anyhow::Result<()> {
    let trimmed = secret.trim();
    if trimmed.len() < MIN_INTEGRATION_SECRET_LEN {
        anyhow::bail!("{name} must contain at least {MIN_INTEGRATION_SECRET_LEN} characters");
    }
    if is_known_weak_secret(trimmed) {
        anyhow::bail!("{name} uses a forbidden weak placeholder value");
    }
    Ok(())
}

fn is_known_weak_secret(value: &str) -> bool {
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

fn generate_app_password_secret() -> String {
    format!(
        "lpeapp-{}-{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

#[cfg(test)]
mod tests {
    use super::{
        bootstrap_admin_request_from_env, bootstrap_admin_request_from_env_or_defaults,
        build_smtp_submission_input_for_owner, ha_activation_check, ha_allows_active_work,
        integration_shared_secret, merge_smtp_bcc_recipients, parse_smtp_submission_sender,
        validate_uploaded_pst_file_with_validator,
    };
    use lpe_domain::SmtpSubmissionRequest;
    use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
    use lpe_mail_auth::AccountPrincipal;
    use lpe_storage::{
        mail::parse_rfc822_message, SubmissionAccountIdentity, SubmittedRecipientInput,
    };
    use std::{
        fs,
        path::PathBuf,
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    };
    use uuid::Uuid;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Debug, Clone)]
    struct FakeDetector {
        detection: MagikaDetection,
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
            Ok(self.detection.clone())
        }
    }

    fn temp_file(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("lpe-pst-upload-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn pst_upload_validation_accepts_valid_pst_like_file() {
        let path = temp_file("mailbox.pst");
        fs::write(&path, b"LPE-PST-V1\n").unwrap();
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "pst".to_string(),
                    mime_type: "application/vnd.ms-outlook".to_string(),
                    description: "pst".to_string(),
                    group: "archive".to_string(),
                    extensions: vec!["pst".to_string()],
                    score: Some(0.99),
                },
            },
            0.80,
        );

        validate_uploaded_pst_file_with_validator(
            &validator,
            &path,
            "mailbox.pst",
            Some("application/vnd.ms-outlook"),
        )
        .unwrap();

        assert!(path.exists());
        assert!(path.with_extension("pst.magika.json").exists());
    }

    #[test]
    fn pst_upload_validation_rejects_extension_and_type_mismatch() {
        let path = temp_file("mailbox.pdf");
        fs::write(&path, b"not a pst").unwrap();
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "pdf".to_string(),
                    mime_type: "application/pdf".to_string(),
                    description: "pdf".to_string(),
                    group: "document".to_string(),
                    extensions: vec!["pdf".to_string()],
                    score: Some(0.99),
                },
            },
            0.80,
        );

        let error = validate_uploaded_pst_file_with_validator(
            &validator,
            &path,
            "mailbox.pdf",
            Some("application/pdf"),
        )
        .unwrap_err();

        assert!(error.to_string().contains("PST upload blocked"));
        assert!(!path.exists());
    }

    #[test]
    fn integration_secret_rejects_missing_or_weak_values() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
        assert!(integration_shared_secret().is_err());

        std::env::set_var("LPE_INTEGRATION_SHARED_SECRET", "change-me");
        assert!(integration_shared_secret().is_err());

        std::env::set_var(
            "LPE_INTEGRATION_SHARED_SECRET",
            "0123456789abcdef0123456789abcdef",
        );
        assert_eq!(
            integration_shared_secret().unwrap(),
            "0123456789abcdef0123456789abcdef"
        );
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
    }

    #[test]
    fn bootstrap_request_requires_explicit_strong_password() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("LPE_BOOTSTRAP_ADMIN_EMAIL", "admin@example.test");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_PASSWORD");
        assert!(bootstrap_admin_request_from_env().is_err());

        std::env::set_var("LPE_BOOTSTRAP_ADMIN_PASSWORD", "change-me");
        assert!(bootstrap_admin_request_from_env().is_err());

        std::env::set_var(
            "LPE_BOOTSTRAP_ADMIN_PASSWORD",
            "Very-Strong-Bootstrap-Password-2026",
        );
        let request = bootstrap_admin_request_from_env().unwrap();
        assert_eq!(request.email, "admin@example.test");
        assert_eq!(request.display_name, "Bootstrap Administrator");

        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_EMAIL");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_PASSWORD");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME");
    }

    #[test]
    fn bootstrap_auto_request_requires_explicit_bootstrap_credentials() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_EMAIL");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_PASSWORD");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME");

        assert!(bootstrap_admin_request_from_env_or_defaults().is_err());

        std::env::set_var("LPE_BOOTSTRAP_ADMIN_EMAIL", "root@tenant.example");
        std::env::set_var(
            "LPE_BOOTSTRAP_ADMIN_PASSWORD",
            "Very-Strong-Bootstrap-Password-2026",
        );
        let request = bootstrap_admin_request_from_env_or_defaults().unwrap();
        assert_eq!(request.email, "root@tenant.example");
        assert_eq!(request.password, "Very-Strong-Bootstrap-Password-2026");

        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_EMAIL");
        std::env::remove_var("LPE_BOOTSTRAP_ADMIN_PASSWORD");
    }

    #[test]
    fn ha_role_check_accepts_only_active_role() {
        let _guard = ENV_LOCK.lock().unwrap();
        let role_file = temp_file("ha-role");

        std::env::set_var("LPE_HA_ROLE_FILE", &role_file);

        fs::write(&role_file, b"active\n").unwrap();
        let active = ha_activation_check();
        assert_eq!(active.status, "ok");

        fs::write(&role_file, b"standby\n").unwrap();
        let standby = ha_activation_check();
        assert_eq!(standby.status, "failed");
        assert!(standby.detail.contains("standby"));

        fs::write(&role_file, b"broken\n").unwrap();
        let invalid = ha_activation_check();
        assert_eq!(invalid.status, "failed");
        assert!(invalid.detail.contains("unsupported role"));

        std::env::remove_var("LPE_HA_ROLE_FILE");
    }

    #[test]
    fn ha_active_work_follows_role_file() {
        let _guard = ENV_LOCK.lock().unwrap();
        let role_file = temp_file("ha-active-work");

        std::env::remove_var("LPE_HA_ROLE_FILE");
        assert!(ha_allows_active_work().unwrap());

        std::env::set_var("LPE_HA_ROLE_FILE", &role_file);
        fs::write(&role_file, b"active\n").unwrap();
        assert!(ha_allows_active_work().unwrap());

        fs::write(&role_file, b"maintenance\n").unwrap();
        assert!(!ha_allows_active_work().unwrap());

        std::env::remove_var("LPE_HA_ROLE_FILE");
    }

    #[test]
    fn smtp_submission_derives_envelope_only_recipients_as_bcc() {
        let raw = concat!(
            "From: Alice <alice@example.test>\r\n",
            "To: Bob <bob@example.test>\r\n",
            "Bcc: Hidden <hidden@example.test>\r\n",
            "Subject: Hi\r\n",
            "\r\n",
            "Body\r\n"
        );

        let bcc = merge_smtp_bcc_recipients(
            raw.as_bytes(),
            &[
                "bob@example.test".to_string(),
                "hidden@example.test".to_string(),
                "blind2@example.test".to_string(),
            ],
            &[lpe_storage::SubmittedRecipientInput {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            &[],
        );

        assert_eq!(bcc.len(), 2);
        assert_eq!(bcc[0].address, "hidden@example.test");
        assert_eq!(bcc[1].address, "blind2@example.test");
    }

    #[test]
    fn smtp_submission_builds_canonical_submit_input() {
        let principal = AccountPrincipal {
            tenant_id: "tenant-a".to_string(),
            account_id: Uuid::nil(),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
        };
        let request = SmtpSubmissionRequest {
            trace_id: "trace-1".to_string(),
            helo: "laptop.example.test".to_string(),
            peer: "203.0.113.55:41234".to_string(),
            account_id: Uuid::nil(),
            account_email: "alice@example.test".to_string(),
            account_display_name: "Alice".to_string(),
            mail_from: "alice@example.test".to_string(),
            rcpt_to: vec![
                "bob@example.test".to_string(),
                "blind@example.test".to_string(),
            ],
            raw_message: concat!(
                "From: Alice <alice@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Hello\r\n",
                "\r\n",
                "Body\r\n"
            )
            .as_bytes()
            .to_vec(),
        };
        let parsed = parse_rfc822_message(&request.raw_message).unwrap();
        let sender =
            parse_smtp_submission_sender(&request.raw_message, &principal.email, &principal.email, &principal.email)
                .unwrap();
        let input = build_smtp_submission_input_for_owner(
            &principal,
            &SubmissionAccountIdentity {
                account_id: principal.account_id,
                email: principal.email.clone(),
                display_name: principal.display_name.clone(),
            },
            &request,
            parsed,
            vec![SubmittedRecipientInput {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            Vec::new(),
            vec![SubmittedRecipientInput {
                address: "blind@example.test".to_string(),
                display_name: None,
            }],
            sender,
        );

        assert_eq!(input.source, "smtp-submission");
        assert_eq!(input.from_address, "alice@example.test");
        assert_eq!(input.to.len(), 1);
        assert_eq!(input.to[0].address, "bob@example.test");
        assert_eq!(input.bcc.len(), 1);
        assert_eq!(input.bcc[0].address, "blind@example.test");
    }

    #[test]
    fn smtp_submission_builds_send_as_input_for_delegated_mailbox() {
        let principal = AccountPrincipal {
            tenant_id: "tenant-a".to_string(),
            account_id: Uuid::new_v4(),
            email: "delegate@example.test".to_string(),
            display_name: "Delegate".to_string(),
        };
        let owner = SubmissionAccountIdentity {
            account_id: Uuid::new_v4(),
            email: "shared@example.test".to_string(),
            display_name: "Shared Mailbox".to_string(),
        };
        let request = SmtpSubmissionRequest {
            trace_id: "trace-2".to_string(),
            helo: "laptop.example.test".to_string(),
            peer: "203.0.113.55:41234".to_string(),
            account_id: principal.account_id,
            account_email: principal.email.clone(),
            account_display_name: principal.display_name.clone(),
            mail_from: "shared@example.test".to_string(),
            rcpt_to: vec!["bob@example.test".to_string()],
            raw_message: concat!(
                "From: Shared Mailbox <shared@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Hello\r\n",
                "\r\n",
                "Body\r\n"
            )
            .as_bytes()
            .to_vec(),
        };

        let parsed = parse_rfc822_message(&request.raw_message).unwrap();
        let sender = parse_smtp_submission_sender(
            &request.raw_message,
            &owner.email,
            &principal.email,
            &owner.email,
        )
        .unwrap();
        let input = build_smtp_submission_input_for_owner(
            &principal,
            &owner,
            &request,
            parsed,
            vec![SubmittedRecipientInput {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            Vec::new(),
            Vec::new(),
            sender,
        );

        assert_eq!(input.account_id, owner.account_id);
        assert_eq!(input.submitted_by_account_id, principal.account_id);
        assert_eq!(input.from_address, owner.email);
        assert_eq!(input.sender_address, None);
    }

    #[test]
    fn smtp_submission_builds_send_on_behalf_input_for_delegated_mailbox() {
        let principal = AccountPrincipal {
            tenant_id: "tenant-a".to_string(),
            account_id: Uuid::new_v4(),
            email: "delegate@example.test".to_string(),
            display_name: "Delegate".to_string(),
        };
        let owner = SubmissionAccountIdentity {
            account_id: Uuid::new_v4(),
            email: "shared@example.test".to_string(),
            display_name: "Shared Mailbox".to_string(),
        };
        let request = SmtpSubmissionRequest {
            trace_id: "trace-3".to_string(),
            helo: "laptop.example.test".to_string(),
            peer: "203.0.113.55:41234".to_string(),
            account_id: principal.account_id,
            account_email: principal.email.clone(),
            account_display_name: principal.display_name.clone(),
            mail_from: "delegate@example.test".to_string(),
            rcpt_to: vec!["bob@example.test".to_string()],
            raw_message: concat!(
                "From: Shared Mailbox <shared@example.test>\r\n",
                "Sender: Delegate <delegate@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Hello\r\n",
                "\r\n",
                "Body\r\n"
            )
            .as_bytes()
            .to_vec(),
        };

        let parsed = parse_rfc822_message(&request.raw_message).unwrap();
        let sender = parse_smtp_submission_sender(
            &request.raw_message,
            &owner.email,
            &principal.email,
            &owner.email,
        )
        .unwrap();
        let input = build_smtp_submission_input_for_owner(
            &principal,
            &owner,
            &request,
            parsed,
            vec![SubmittedRecipientInput {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            Vec::new(),
            Vec::new(),
            sender,
        );

        assert_eq!(input.account_id, owner.account_id);
        assert_eq!(input.submitted_by_account_id, principal.account_id);
        assert_eq!(input.from_address, owner.email);
        assert_eq!(input.sender_address.as_deref(), Some("delegate@example.test"));
        assert_eq!(input.sender_display.as_deref(), Some("Delegate"));
    }
}

fn admin_session_minutes() -> u32 {
    env::var("LPE_ADMIN_SESSION_MINUTES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(45)
}

fn client_session_minutes() -> u32 {
    env::var("LPE_CLIENT_SESSION_MINUTES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(720)
}

fn client_oauth_access_token_seconds() -> u32 {
    env::var("LPE_MAIL_OAUTH_ACCESS_TOKEN_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value >= 60)
        .unwrap_or(DEFAULT_OAUTH_ACCESS_TOKEN_SECONDS)
}

fn hash_password(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::encode_b64(Uuid::new_v4().as_bytes())
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .to_string())
}

fn verify_password(password_hash: &str, password: &str) -> bool {
    PasswordHash::new(password_hash)
        .ok()
        .and_then(|parsed| {
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .ok()
        })
        .is_some()
}
