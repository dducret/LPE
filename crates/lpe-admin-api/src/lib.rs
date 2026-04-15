use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use lpe_ai::{LocalModelProvider, StubLocalModelProvider};
use lpe_core::CoreService;
use lpe_storage::{
    AdminDashboard, AuditEntryInput, DashboardUpdate, EmailTraceResult, EmailTraceSearchInput,
    HealthResponse, LocalAiSettings, NewAccount, NewAlias, NewDomain, NewFilterRule, NewMailbox,
    NewServerAdministrator, SecuritySettings, ServerSettings, Storage,
};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Deserialize)]
struct CreateAccountRequest {
    email: String,
    display_name: String,
    quota_mb: u32,
}

#[derive(Debug, Deserialize)]
struct CreateMailboxRequest {
    account_id: Uuid,
    display_name: String,
    role: String,
    retention_days: u16,
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

type ApiResult<T> = std::result::Result<Json<T>, (StatusCode, String)>;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/bootstrap/admin", get(bootstrap_admin))
        .route("/health/local-ai", get(local_ai_health))
        .route("/capabilities/attachments", get(attachment_support))
        .route("/console/dashboard", get(dashboard))
        .route("/console/accounts", post(create_account))
        .route("/console/mailboxes", post(create_mailbox))
        .route("/console/domains", post(create_domain))
        .route("/console/aliases", post(create_alias))
        .route("/console/admins", post(create_server_administrator))
        .route("/console/antispam/rules", post(create_filter_rule))
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

async fn bootstrap_admin(State(storage): State<Storage>) -> ApiResult<BootstrapResponse> {
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

async fn dashboard(State(storage): State<Storage>) -> ApiResult<AdminDashboard> {
    Ok(Json(
        storage
            .fetch_admin_dashboard()
            .await
            .map_err(internal_error)?,
    ))
}

async fn create_account(
    State(storage): State<Storage>,
    Json(request): Json<CreateAccountRequest>,
) -> ApiResult<AdminDashboard> {
    storage
        .create_account(
            NewAccount {
                email: request.email,
                display_name: request.display_name,
                quota_mb: request.quota_mb.max(256),
            },
            AuditEntryInput {
                actor: "admin@example.test".to_string(),
                action: "create-account".to_string(),
                subject: "account created from admin console".to_string(),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn create_mailbox(
    State(storage): State<Storage>,
    Json(request): Json<CreateMailboxRequest>,
) -> ApiResult<AdminDashboard> {
    storage
        .create_mailbox(
            NewMailbox {
                account_id: request.account_id,
                display_name: request.display_name.clone(),
                role: request.role,
                retention_days: request.retention_days.max(1),
            },
            AuditEntryInput {
                actor: "admin@example.test".to_string(),
                action: "create-mailbox".to_string(),
                subject: format!("{} for {}", request.display_name, request.account_id),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn create_domain(
    State(storage): State<Storage>,
    Json(request): Json<CreateDomainRequest>,
) -> ApiResult<AdminDashboard> {
    storage
        .create_domain(
            NewDomain {
                name: request.name.clone(),
                default_quota_mb: request.default_quota_mb.max(256),
                inbound_enabled: request.inbound_enabled,
                outbound_enabled: request.outbound_enabled,
            },
            AuditEntryInput {
                actor: "admin@example.test".to_string(),
                action: "create-domain".to_string(),
                subject: request.name,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn create_alias(
    State(storage): State<Storage>,
    Json(request): Json<CreateAliasRequest>,
) -> ApiResult<AdminDashboard> {
    storage
        .create_alias(
            NewAlias {
                source: request.source.clone(),
                target: request.target,
                kind: request.kind,
            },
            AuditEntryInput {
                actor: "admin@example.test".to_string(),
                action: "create-alias".to_string(),
                subject: request.source,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn update_server_settings(
    State(storage): State<Storage>,
    Json(request): Json<UpdateServerSettingsRequest>,
) -> ApiResult<AdminDashboard> {
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
                actor: "admin@example.test".to_string(),
                action: "update-server-settings".to_string(),
                subject: request.primary_hostname,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn update_security_settings(
    State(storage): State<Storage>,
    Json(request): Json<UpdateSecuritySettingsRequest>,
) -> ApiResult<AdminDashboard> {
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
                actor: "admin@example.test".to_string(),
                action: "update-security-settings".to_string(),
                subject: "admin policies".to_string(),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn update_local_ai_settings(
    State(storage): State<Storage>,
    Json(request): Json<UpdateLocalAiSettingsRequest>,
) -> ApiResult<AdminDashboard> {
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
                actor: "admin@example.test".to_string(),
                action: "update-local-ai-settings".to_string(),
                subject: request.model,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn update_antispam_settings(
    State(storage): State<Storage>,
    Json(request): Json<UpdateAntispamSettingsRequest>,
) -> ApiResult<AdminDashboard> {
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
                actor: "admin@example.test".to_string(),
                action: "update-antispam-settings".to_string(),
                subject: request.spam_engine,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn create_server_administrator(
    State(storage): State<Storage>,
    Json(request): Json<CreateServerAdministratorRequest>,
) -> ApiResult<AdminDashboard> {
    storage
        .create_server_administrator(
            NewServerAdministrator {
                domain_id: request.domain_id,
                email: request.email,
                display_name: request.display_name,
                role: request.role,
                rights_summary: request.rights_summary,
            },
            AuditEntryInput {
                actor: "admin@example.test".to_string(),
                action: "create-server-administrator".to_string(),
                subject: "domain delegation".to_string(),
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn create_filter_rule(
    State(storage): State<Storage>,
    Json(request): Json<CreateFilterRuleRequest>,
) -> ApiResult<AdminDashboard> {
    storage
        .create_filter_rule(
            NewFilterRule {
                name: request.name.clone(),
                scope: request.scope,
                action: request.action,
                status: request.status,
            },
            AuditEntryInput {
                actor: "admin@example.test".to_string(),
                action: "create-antispam-rule".to_string(),
                subject: request.name,
            },
        )
        .await
        .map_err(internal_error)?;

    dashboard(State(storage)).await
}

async fn search_email_trace(
    State(storage): State<Storage>,
    Json(request): Json<EmailTraceSearchRequest>,
) -> ApiResult<Vec<EmailTraceResult>> {
    Ok(Json(
        storage
            .search_email_trace(EmailTraceSearchInput {
                query: request.query,
            })
            .await
            .map_err(internal_error)?,
    ))
}

fn internal_error(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}
