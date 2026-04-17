use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post, put},
    Json, Router,
};
use lpe_ai::{LocalModelProvider, StubLocalModelProvider};
use lpe_core::CoreService;
use lpe_storage::{
    AccountCredentialInput, AdminCredentialInput, AdminDashboard, AuditEntryInput,
    AuthenticatedAdmin, DashboardUpdate, EmailTraceResult, EmailTraceSearchInput, HealthResponse,
    LocalAiSettings, NewAccount, NewAlias, NewDomain, NewFilterRule, NewMailbox, NewPstTransferJob,
    NewServerAdministrator, PstJobExecutionSummary, SecuritySettings, ServerSettings, Storage,
    SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput,
};
use serde::{Deserialize, Serialize};
use std::env;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
struct BootstrapResponse {
    email: String,
    display_name: String,
}

#[derive(Debug, Clone, Serialize)]
struct LocalAiHealthResponse {
    provider: String,
    models: Vec<String>,
    bootstrap_summary_payload: String,
    enabled: bool,
    offline_only: bool,
}

#[derive(Debug, Clone, Serialize)]
struct AttachmentSupportResponse {
    formats: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct LoginResponse {
    token: String,
    admin: AuthenticatedAdmin,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct CreateAccountRequest {
    email: String,
    display_name: String,
    quota_mb: u32,
    password: String,
}

#[derive(Debug, Deserialize)]
struct CreateMailboxRequest {
    account_id: Uuid,
    display_name: String,
    role: String,
    retention_days: u16,
}

#[derive(Debug, Deserialize)]
struct CreatePstTransferJobRequest {
    mailbox_id: Uuid,
    direction: String,
    server_path: String,
    requested_by: String,
}

#[derive(Debug, Deserialize)]
struct CreateDomainRequest {
    name: String,
    default_quota_mb: u32,
    inbound_enabled: bool,
    outbound_enabled: bool,
}

#[derive(Debug, Deserialize)]
struct CreateAliasRequest {
    source: String,
    target: String,
    kind: String,
}

#[derive(Debug, Deserialize)]
struct UpdateServerSettingsRequest {
    primary_hostname: String,
    admin_bind_address: String,
    smtp_bind_address: String,
    imap_bind_address: String,
    jmap_bind_address: String,
    default_locale: String,
    max_message_size_mb: u32,
    tls_mode: String,
}

#[derive(Debug, Deserialize)]
struct UpdateSecuritySettingsRequest {
    password_login_enabled: bool,
    mfa_required_for_admins: bool,
    session_timeout_minutes: u32,
    audit_retention_days: u32,
}

#[derive(Debug, Deserialize)]
struct UpdateLocalAiSettingsRequest {
    enabled: bool,
    provider: String,
    model: String,
    offline_only: bool,
    indexing_enabled: bool,
}

#[derive(Debug, Deserialize)]
struct UpdateAntispamSettingsRequest {
    content_filtering_enabled: bool,
    spam_engine: String,
    quarantine_enabled: bool,
    quarantine_retention_days: u32,
}

#[derive(Debug, Deserialize)]
struct CreateServerAdministratorRequest {
    domain_id: Option<Uuid>,
    email: String,
    display_name: String,
    role: String,
    rights_summary: String,
    password: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateFilterRuleRequest {
    name: String,
    scope: String,
    action: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct EmailTraceSearchRequest {
    query: String,
}

#[derive(Debug, Deserialize)]
struct SubmitMessageRequest {
    account_id: Uuid,
    source: Option<String>,
    from_display: Option<String>,
    from_address: String,
    to: Vec<SubmitRecipientRequest>,
    cc: Option<Vec<SubmitRecipientRequest>>,
    bcc: Option<Vec<SubmitRecipientRequest>>,
    subject: String,
    body_text: String,
    body_html_sanitized: Option<String>,
    internet_message_id: Option<String>,
    mime_blob_ref: Option<String>,
    size_octets: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SubmitRecipientRequest {
    address: String,
    display_name: Option<String>,
}

type ApiResult<T> = std::result::Result<Json<T>, (StatusCode, String)>;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/bootstrap/admin", get(bootstrap_admin))
        .route("/health/local-ai", get(local_ai_health))
        .route("/capabilities/attachments", get(attachment_support))
        .route("/console/dashboard", get(dashboard))
        .route("/console/accounts", post(create_account))
        .route("/console/mailboxes", post(create_mailbox))
        .route("/console/mailboxes/pst-jobs", post(create_pst_transfer_job))
        .route("/console/domains", post(create_domain))
        .route("/console/aliases", post(create_alias))
        .route("/console/admins", post(create_server_administrator))
        .route("/console/antispam/rules", post(create_filter_rule))
        .route(
            "/console/mailboxes/pst-jobs/run-pending",
            post(run_pst_jobs),
        )
        .route("/mail/messages/submit", post(submit_message))
        .route(
            "/console/audit/email-trace-search",
            post(search_email_trace),
        )
        .route("/console/settings/server", put(update_server_settings))
        .route("/console/settings/security", put(update_security_settings))
        .route("/console/settings/local-ai", put(update_local_ai_settings))
        .route("/console/settings/antispam", put(update_antispam_settings))
        .with_state(storage)
}

async fn health(State(storage): State<Storage>) -> ApiResult<HealthResponse> {
    let dashboard = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    Ok(Json(dashboard.health))
}

async fn login(
    State(storage): State<Storage>,
    Json(request): Json<LoginRequest>,
) -> ApiResult<LoginResponse> {
    ensure_bootstrap_admin(&storage)
        .await
        .map_err(internal_error)?;
    let email = request.email.trim().to_lowercase();
    let candidate = storage
        .fetch_admin_login(&email)
        .await
        .map_err(internal_error)?
        .ok_or((StatusCode::UNAUTHORIZED, "invalid credentials".to_string()))?;

    if candidate.status != "active" || !verify_password(&candidate.password_hash, &request.password)
    {
        return Err((StatusCode::UNAUTHORIZED, "invalid credentials".to_string()));
    }

    let token = Uuid::new_v4().to_string();
    storage
        .create_admin_session(&token, &email, admin_session_minutes())
        .await
        .map_err(internal_error)?;
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

async fn bootstrap_admin(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<BootstrapResponse> {
    require_admin(&storage, &headers, "dashboard").await?;
    let dashboard = storage
        .fetch_admin_dashboard()
        .await
        .map_err(internal_error)?;
    let bootstrap = dashboard
        .accounts
        .first()
        .cloned()
        .map(|account| BootstrapResponse {
            email: account.email,
            display_name: account.display_name,
        })
        .unwrap_or(BootstrapResponse {
            email: "admin@example.test".to_string(),
            display_name: "LPE Administrator".to_string(),
        });

    Ok(Json(bootstrap))
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
    let admin = require_admin(&storage, &headers, "mail").await?;
    let subject_for_audit = request.subject.clone();
    let submitted = storage
        .submit_message(
            SubmitMessageInput {
                account_id: request.account_id,
                source: request.source.unwrap_or_else(|| "jmap".to_string()),
                from_display: request.from_display,
                from_address: request.from_address,
                to: map_recipients(request.to),
                cc: map_recipients(request.cc.unwrap_or_default()),
                bcc: map_recipients(request.bcc.unwrap_or_default()),
                subject: request.subject,
                body_text: request.body_text,
                body_html_sanitized: request.body_html_sanitized,
                internet_message_id: request.internet_message_id,
                mime_blob_ref: request.mime_blob_ref,
                size_octets: request.size_octets.unwrap_or(0),
            },
            AuditEntryInput {
                actor: admin.email,
                action: "submit-message".to_string(),
                subject: subject_for_audit,
            },
        )
        .await
        .map_err(internal_error)?;

    Ok(Json(submitted))
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

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(ToString::to_string)
}

fn admin_has_right(admin: &AuthenticatedAdmin, right: &str) -> bool {
    if admin.role == "server-admin" || admin.role == "super-admin" {
        return true;
    }

    admin
        .rights_summary
        .split(',')
        .map(|entry| entry.trim().to_lowercase())
        .any(|entry| entry == right || entry == "*")
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

async fn ensure_bootstrap_admin(storage: &Storage) -> anyhow::Result<()> {
    let email =
        env::var("LPE_BOOTSTRAP_ADMIN_EMAIL").unwrap_or_else(|_| "admin@example.test".to_string());
    let password =
        env::var("LPE_BOOTSTRAP_ADMIN_PASSWORD").unwrap_or_else(|_| "change-me".to_string());

    if storage.fetch_admin_login(&email).await?.is_some() {
        return Ok(());
    }

    storage
        .create_server_administrator(
            NewServerAdministrator {
                domain_id: None,
                email: email.clone(),
                display_name: "Bootstrap Administrator".to_string(),
                role: "server-admin".to_string(),
                rights_summary:
                    "server, domains, accounts, aliases, admins, policies, security, ai, antispam, pst, audit, mail"
                        .to_string(),
            },
            AuditEntryInput {
                actor: "bootstrap".to_string(),
                action: "create-bootstrap-admin".to_string(),
                subject: email.clone(),
            },
        )
        .await?;

    storage
        .upsert_admin_credential(
            AdminCredentialInput {
                email: email.clone(),
                password_hash: hash_password(&password)?,
            },
            AuditEntryInput {
                actor: "bootstrap".to_string(),
                action: "set-bootstrap-password".to_string(),
                subject: email,
            },
        )
        .await?;

    Ok(())
}

fn admin_session_minutes() -> u32 {
    env::var("LPE_ADMIN_SESSION_MINUTES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(45)
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
