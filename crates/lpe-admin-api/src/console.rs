use axum::{
    extract::{Multipart, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use lpe_ai::{LocalModelProvider, StubLocalModelProvider};
use lpe_core::CoreService;
use lpe_storage::{
    AccountCredentialInput, AdminCredentialInput, AdminDashboard, AuditEntryInput, DashboardUpdate,
    EmailTraceResult, EmailTraceSearchInput, LocalAiSettings, NewAccount, NewAlias, NewDomain,
    NewFilterRule, NewMailbox, NewPstTransferJob, NewServerAdministrator, PstJobExecutionSummary,
    SecuritySettings, ServerSettings, Storage, UpdateAccount, UpdateDomain,
};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::{
    http::{bad_request_error, internal_error},
    pst::{pst_import_dir, sanitize_upload_filename, validate_uploaded_pst_file},
    require_admin,
    security::hash_password,
    types::{
        ApiResult, AttachmentSupportResponse, CreateAccountRequest, CreateAliasRequest,
        CreateDomainRequest, CreateFilterRuleRequest, CreateMailboxRequest,
        CreatePstTransferJobRequest, CreateServerAdministratorRequest, EmailTraceSearchRequest,
        LocalAiHealthResponse, MailFlowResponse, UpdateAccountRequest,
        UpdateAntispamSettingsRequest, UpdateDomainRequest, UpdateLocalAiSettingsRequest,
        UpdateSecuritySettingsRequest, UpdateServerSettingsRequest,
    },
    util::{ensure_admin_can_manage_email, mailbox_account_email},
};

pub(crate) async fn local_ai_health(
    State(storage): State<Storage>,
) -> ApiResult<LocalAiHealthResponse> {
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

pub(crate) async fn attachment_support(_: State<Storage>) -> ApiResult<AttachmentSupportResponse> {
    Ok(Json(AttachmentSupportResponse {
        formats: vec!["PDF".to_string(), "DOCX".to_string(), "ODT".to_string()],
    }))
}

pub(crate) async fn dashboard(
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

pub(crate) async fn create_account(
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

pub(crate) async fn update_account(
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

pub(crate) async fn create_mailbox(
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

pub(crate) async fn create_pst_transfer_job(
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

pub(crate) async fn upload_pst_import(
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
    let mut uploaded_path: Option<std::path::PathBuf> = None;

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

pub(crate) async fn create_domain(
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

pub(crate) async fn update_domain(
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

pub(crate) async fn create_alias(
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

pub(crate) async fn update_server_settings(
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

pub(crate) async fn update_security_settings(
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

pub(crate) async fn update_local_ai_settings(
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

pub(crate) async fn update_antispam_settings(
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

pub(crate) async fn create_server_administrator(
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

pub(crate) async fn create_filter_rule(
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

pub(crate) async fn search_email_trace(
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

pub(crate) async fn run_pst_jobs(
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

pub(crate) async fn mail_flow(
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
