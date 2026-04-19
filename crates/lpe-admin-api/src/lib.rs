use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, post, put},
    Json, Router,
};
use lpe_ai::{LocalModelProvider, StubLocalModelProvider};
use lpe_core::CoreService;
use lpe_domain::{InboundDeliveryRequest, InboundDeliveryResponse};
use lpe_magika::{
    write_validation_record, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_storage::{
    AccountCredentialInput, AdminCredentialInput, AdminDashboard, AuditEntryInput,
    AuthenticatedAccount, AuthenticatedAdmin, ClientContact, ClientEvent, ClientWorkspace,
    DashboardUpdate, EmailTraceResult, EmailTraceSearchInput, HealthResponse, LocalAiSettings,
    NewAccount, NewAlias, NewDomain, NewFilterRule, NewMailbox, NewPstTransferJob,
    NewServerAdministrator, PstJobExecutionSummary, SavedDraftMessage, SecuritySettings,
    ServerSettings, Storage, SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput,
    UpdateAccount, UpsertClientContactInput, UpsertClientEventInput,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

const MIN_ADMIN_PASSWORD_LEN: usize = 12;
const MIN_INTEGRATION_SECRET_LEN: usize = 32;

#[derive(Debug, Clone)]
pub struct BootstrapAdminRequest {
    pub email: String,
    pub display_name: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BootstrapAdminResponse {
    pub email: String,
    pub display_name: String,
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

#[derive(Debug, Clone, Serialize)]
struct ClientLoginResponse {
    token: String,
    account: AuthenticatedAccount,
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
struct UpdateAccountRequest {
    display_name: String,
    quota_mb: u32,
    status: String,
    password: Option<String>,
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
    draft_message_id: Option<Uuid>,
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

#[derive(Debug, Deserialize)]
struct UpsertClientContactRequest {
    id: Option<Uuid>,
    name: String,
    role: String,
    email: String,
    phone: String,
    team: String,
    notes: String,
}

#[derive(Debug, Deserialize)]
struct UpsertClientEventRequest {
    id: Option<Uuid>,
    date: String,
    time: String,
    title: String,
    location: String,
    attendees: String,
    notes: String,
}

type ApiResult<T> = std::result::Result<Json<T>, (StatusCode, String)>;

pub fn router(storage: Storage) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/mail/auth/login", post(client_login))
        .route("/mail/auth/logout", post(client_logout))
        .route("/mail/auth/me", get(client_me))
        .route("/mail/workspace", get(client_workspace))
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
        .route("/console/aliases", post(create_alias))
        .route("/console/admins", post(create_server_administrator))
        .route("/console/antispam/rules", post(create_filter_rule))
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
            "/mail/messages/{message_id}/draft",
            delete(delete_draft_message),
        )
        .route("/mail/contacts", post(upsert_client_contact))
        .route("/mail/calendar/events", post(upsert_client_event))
        .route(
            "/console/audit/email-trace-search",
            post(search_email_trace),
        )
        .route("/console/settings/server", put(update_server_settings))
        .route("/console/settings/security", put(update_security_settings))
        .route("/console/settings/local-ai", put(update_local_ai_settings))
        .route("/console/settings/antispam", put(update_antispam_settings))
        .nest("/jmap", lpe_jmap::router())
        .merge(lpe_activesync::router())
        .merge(lpe_dav::router())
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

async fn login(
    State(storage): State<Storage>,
    Json(request): Json<LoginRequest>,
) -> ApiResult<LoginResponse> {
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

async fn client_login(
    State(storage): State<Storage>,
    Json(request): Json<LoginRequest>,
) -> ApiResult<ClientLoginResponse> {
    let email = request.email.trim().to_lowercase();
    let candidate = storage
        .fetch_account_login(&email)
        .await
        .map_err(internal_error)?
        .ok_or((StatusCode::UNAUTHORIZED, "invalid credentials".to_string()))?;

    if candidate.status != "active" || !verify_password(&candidate.password_hash, &request.password)
    {
        return Err((StatusCode::UNAUTHORIZED, "invalid credentials".to_string()));
    }

    let token = Uuid::new_v4().to_string();
    storage
        .create_account_session(&token, &candidate.email, client_session_minutes())
        .await
        .map_err(internal_error)?;
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
    let account = require_account(&storage, &headers).await?;
    let subject_for_audit = request.subject.clone();
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
    Ok(Json(
        storage
            .upsert_client_contact(UpsertClientContactInput {
                id: request.id,
                account_id: account.account_id,
                name: request.name,
                role: request.role,
                email: request.email,
                phone: request.phone,
                team: request.team,
                notes: request.notes,
            })
            .await
            .map_err(bad_request_error)?,
    ))
}

async fn upsert_client_event(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertClientEventRequest>,
) -> ApiResult<ClientEvent> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .upsert_client_event(UpsertClientEventInput {
                id: request.id,
                account_id: account.account_id,
                date: request.date,
                time: request.time,
                title: request.title,
                location: request.location,
                attendees: request.attendees,
                notes: request.notes,
            })
            .await
            .map_err(bad_request_error)?,
    ))
}

async fn deliver_inbound_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<InboundDeliveryRequest>,
) -> ApiResult<InboundDeliveryResponse> {
    require_integration(&headers)?;
    Ok(Json(
        storage
            .deliver_inbound_message(request)
            .await
            .map_err(bad_request_error)?,
    ))
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

fn require_integration(headers: &HeaderMap) -> std::result::Result<(), (StatusCode, String)> {
    let provided = headers
        .get("x-lpe-integration-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "missing integration key".to_string(),
        ))?;
    let expected = integration_shared_secret().map_err(internal_error)?;
    if provided == expected {
        Ok(())
    } else {
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

#[cfg(test)]
mod tests {
    use super::{
        bootstrap_admin_request_from_env, integration_shared_secret,
        validate_uploaded_pst_file_with_validator,
    };
    use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
    use std::{
        fs,
        path::PathBuf,
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    };

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
