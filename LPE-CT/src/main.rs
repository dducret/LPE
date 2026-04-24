use anyhow::{Context, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    middleware,
    routing::{get, post, put},
    Json, Router,
};
use lpe_domain::{
    current_unix_timestamp, BridgeAuthError, OutboundMessageHandoffRequest,
    OutboundMessageHandoffResponse, SignedIntegrationHeaders, DEFAULT_MAX_SKEW_SECONDS,
    INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER,
    INTEGRATION_TIMESTAMP_HEADER,
};
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tracing::{info, warn};
use uuid::Uuid;

mod dkim_signing;
mod observability;
mod reporting;
mod smtp;
mod storage;
mod submission;
mod transport_policy;

const MIN_INTEGRATION_SECRET_LEN: usize = 32;
const OUTBOUND_HANDOFF_PATH: &str = "/api/v1/integration/outbound-messages";

static INTEGRATION_REPLAY_CACHE: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<String, i64>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub(crate) fn env_test_lock() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SiteProfile {
    node_name: String,
    role: String,
    region: String,
    dmz_zone: String,
    published_mx: String,
    management_fqdn: String,
    public_smtp_bind: String,
    management_bind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RelaySettings {
    #[serde(default = "default_true")]
    ha_enabled: bool,
    primary_upstream: String,
    secondary_upstream: String,
    #[serde(default = "default_core_delivery_base_url")]
    core_delivery_base_url: String,
    mutual_tls_required: bool,
    fallback_to_hold_queue: bool,
    sync_interval_seconds: u32,
    lan_dependency_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RoutingSettings {
    #[serde(default)]
    rules: Vec<RoutingRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoutingRule {
    id: String,
    description: String,
    sender_domain: Option<String>,
    recipient_domain: Option<String>,
    relay_target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ThrottlingSettings {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    rules: Vec<ThrottleRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThrottleRule {
    id: String,
    scope: String,
    recipient_domain: Option<String>,
    sender_domain: Option<String>,
    max_messages: u32,
    window_seconds: u32,
    retry_after_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetworkSettings {
    allowed_management_cidrs: Vec<String>,
    allowed_upstream_cidrs: Vec<String>,
    outbound_smart_hosts: Vec<String>,
    public_listener_enabled: bool,
    submission_listener_enabled: bool,
    proxy_protocol_enabled: bool,
    max_concurrent_sessions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LocalDataStoresSettings {
    #[serde(default)]
    state_file_path: String,
    #[serde(default)]
    spool_root: String,
    #[serde(default = "default_spool_queues")]
    spool_queues: Vec<String>,
    #[serde(default = "default_policy_artifacts")]
    policy_artifacts: Vec<String>,
    #[serde(default = "default_forbidden_canonical_data")]
    forbidden_canonical_data: Vec<String>,
    #[serde(default)]
    dedicated_postgres: LocalPostgresStore,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LocalPostgresStore {
    enabled: bool,
    #[serde(default = "default_local_db_purposes")]
    purposes: Vec<String>,
    #[serde(default)]
    listen_address: Option<String>,
    #[serde(default = "default_local_db_network_scope")]
    network_scope: String,
    #[serde(default = "default_true")]
    public_exposure_forbidden: bool,
    #[serde(default = "default_local_db_notes")]
    notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AddressPolicySettings {
    #[serde(default)]
    allow_senders: Vec<String>,
    #[serde(default)]
    block_senders: Vec<String>,
    #[serde(default)]
    allow_recipients: Vec<String>,
    #[serde(default)]
    block_recipients: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecipientVerificationSettings {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_true")]
    fail_closed: bool,
    #[serde(default = "default_recipient_verification_cache_ttl_seconds")]
    cache_ttl_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AttachmentPolicySettings {
    #[serde(default)]
    allow_extensions: Vec<String>,
    #[serde(default)]
    block_extensions: Vec<String>,
    #[serde(default)]
    allow_mime_types: Vec<String>,
    #[serde(default)]
    block_mime_types: Vec<String>,
    #[serde(default)]
    allow_detected_types: Vec<String>,
    #[serde(default)]
    block_detected_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DkimDomainConfig {
    domain: String,
    selector: String,
    private_key_path: String,
    #[serde(default = "default_true")]
    enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DkimSettings {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_dkim_headers")]
    headers: Vec<String>,
    #[serde(default = "default_true")]
    over_sign: bool,
    #[serde(default)]
    expiration_seconds: Option<u32>,
    #[serde(default)]
    domains: Vec<DkimDomainConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolicySettings {
    drain_mode: bool,
    quarantine_enabled: bool,
    greylisting_enabled: bool,
    #[serde(default = "default_antivirus_enabled")]
    antivirus_enabled: bool,
    #[serde(default = "default_antivirus_fail_closed")]
    antivirus_fail_closed: bool,
    #[serde(default = "default_antivirus_provider_chain")]
    antivirus_provider_chain: Vec<String>,
    #[serde(default = "default_bayespam_enabled")]
    bayespam_enabled: bool,
    #[serde(default = "default_bayespam_auto_learn")]
    bayespam_auto_learn: bool,
    #[serde(default = "default_bayespam_score_weight")]
    bayespam_score_weight: f32,
    #[serde(default = "default_bayespam_min_token_length")]
    bayespam_min_token_length: u32,
    #[serde(default = "default_bayespam_max_tokens")]
    bayespam_max_tokens: u32,
    require_spf: bool,
    require_dkim_alignment: bool,
    require_dmarc_enforcement: bool,
    #[serde(default = "default_defer_on_auth_tempfail")]
    defer_on_auth_tempfail: bool,
    #[serde(default = "default_dnsbl_enabled")]
    dnsbl_enabled: bool,
    #[serde(default = "default_dnsbl_zones")]
    dnsbl_zones: Vec<String>,
    #[serde(default = "default_reputation_enabled")]
    reputation_enabled: bool,
    #[serde(default = "default_reputation_quarantine_threshold")]
    reputation_quarantine_threshold: i32,
    #[serde(default = "default_reputation_reject_threshold")]
    reputation_reject_threshold: i32,
    #[serde(default = "default_spam_quarantine_threshold")]
    spam_quarantine_threshold: f32,
    #[serde(default = "default_spam_reject_threshold")]
    spam_reject_threshold: f32,
    attachment_text_scan_enabled: bool,
    max_message_size_mb: u32,
    #[serde(default)]
    address_policy: AddressPolicySettings,
    #[serde(default = "default_recipient_verification_settings")]
    recipient_verification: RecipientVerificationSettings,
    #[serde(default)]
    attachment_policy: AttachmentPolicySettings,
    #[serde(default = "default_dkim_settings")]
    dkim: DkimSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpdateSettings {
    channel: String,
    auto_download: bool,
    maintenance_window: String,
    last_applied_release: String,
    update_source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueMetrics {
    inbound_messages: u32,
    deferred_messages: u32,
    quarantined_messages: u32,
    held_messages: u32,
    delivery_attempts_last_hour: u32,
    upstream_reachable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuditEvent {
    timestamp: String,
    actor: String,
    action: String,
    details: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ManagementAuthState {
    admin_email: String,
    password_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DashboardState {
    site: SiteProfile,
    relay: RelaySettings,
    #[serde(default)]
    routing: RoutingSettings,
    #[serde(default)]
    throttling: ThrottlingSettings,
    network: NetworkSettings,
    #[serde(default)]
    local_data_stores: LocalDataStoresSettings,
    policies: PolicySettings,
    #[serde(default = "reporting::default_reporting_settings")]
    reporting: reporting::ReportingSettings,
    updates: UpdateSettings,
    queues: QueueMetrics,
    #[serde(default)]
    management_auth: ManagementAuthState,
    #[serde(default)]
    audit: Vec<AuditEvent>,
}

#[derive(Debug, Clone, Serialize)]
struct HealthResponse {
    status: String,
    service: String,
    node_name: String,
    role: String,
}

#[derive(Debug, Clone, Serialize)]
struct ReadinessCheck {
    name: String,
    status: String,
    critical: bool,
    detail: String,
}

#[derive(Debug, Clone, Serialize)]
struct ReadinessResponse {
    status: String,
    service: String,
    node_name: String,
    role: String,
    warnings: u32,
    checks: Vec<ReadinessCheck>,
}

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<DashboardState>>,
    sessions: Arc<Mutex<std::collections::BTreeMap<String, ManagementSession>>>,
    state_file: Arc<PathBuf>,
    spool_dir: Arc<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Debug, Clone, Serialize)]
struct ManagementIdentity {
    email: String,
    auth_method: String,
}

#[derive(Debug, Clone, Serialize)]
struct LoginResponse {
    token: String,
    admin: ManagementIdentity,
}

#[derive(Debug, Clone, Serialize)]
struct RouteDiagnosticsResponse {
    primary_upstream: String,
    secondary_upstream: String,
    routing: RoutingSettings,
    throttling: ThrottlingSettings,
}

#[derive(Debug, Clone, Serialize)]
struct PolicyStatusResponse {
    recipient_verification: RecipientVerificationStatusView,
    dkim: DkimStatusView,
}

#[derive(Debug, Clone, Serialize)]
struct RecipientVerificationStatusView {
    enabled: bool,
    fail_closed: bool,
    cache_ttl_seconds: u32,
    operational_state: String,
    cache_backend: String,
}

#[derive(Debug, Clone, Serialize)]
struct DkimStatusView {
    enabled: bool,
    operational_state: String,
    headers: Vec<String>,
    over_sign: bool,
    expiration_seconds: Option<u32>,
    domains: Vec<DkimDomainStatusView>,
}

#[derive(Debug, Clone, Serialize)]
struct DkimDomainStatusView {
    domain: String,
    selector: String,
    private_key_path: String,
    enabled: bool,
    key_status: String,
}

#[derive(Debug, Clone)]
struct ManagementSession {
    email: String,
    auth_method: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    observability::init_tracing("lpe-ct");

    let bind_address =
        env::var("LPE_CT_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8380".to_string());
    let smtp_bind_address =
        env::var("LPE_CT_SMTP_BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0:25".to_string());
    let submission_bind_address = env::var("LPE_CT_SUBMISSION_BIND_ADDRESS").ok();
    let state_file = PathBuf::from(
        env::var("LPE_CT_STATE_FILE").unwrap_or_else(|_| "/var/lib/lpe-ct/state.json".to_string()),
    );
    let spool_dir = PathBuf::from(
        env::var("LPE_CT_SPOOL_DIR").unwrap_or_else(|_| "/var/spool/lpe-ct".to_string()),
    );
    smtp::initialize_spool(&spool_dir)?;

    let mut dashboard = load_or_initialize_state(&state_file)?;
    apply_env_overrides(&mut dashboard);
    ensure_management_bootstrap(&mut dashboard)?;
    normalize_policy_settings(&mut dashboard.policies);
    normalize_local_data_stores(&mut dashboard.local_data_stores);
    reporting::normalize_reporting_settings(&mut dashboard.reporting);
    let runtime = smtp::runtime_config_from_dashboard(&dashboard);
    if let Err(error) = smtp::prepare_local_store(&spool_dir, &runtime).await {
        if dashboard.local_data_stores.dedicated_postgres.enabled {
            warn!(
                error = %error,
                "unable to prepare the private LPE-CT PostgreSQL store; continuing with degraded management state"
            );
        } else {
            return Err(error);
        }
    }
    reporting::enforce_retention(&spool_dir, &runtime, &dashboard.reporting).await?;
    dashboard.site.management_bind = bind_address.clone();
    dashboard.site.public_smtp_bind = smtp_bind_address.clone();
    dashboard.network.submission_listener_enabled =
        submission_listener_is_configured(&submission_bind_address);
    persist_state(&state_file, &dashboard)?;
    if let Err(error) =
        storage::sync_dashboard_configuration(&local_db_config_from_dashboard(&dashboard), &dashboard)
            .await
    {
        if dashboard.local_data_stores.dedicated_postgres.enabled {
            warn!(
                error = %error,
                "unable to mirror dashboard configuration into the private LPE-CT PostgreSQL store; continuing with degraded management state"
            );
        } else {
            return Err(error);
        }
    }

    let state = AppState {
        store: Arc::new(Mutex::new(dashboard)),
        sessions: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        state_file: Arc::new(state_file),
        spool_dir: Arc::new(spool_dir),
    };

    let api_state = state.clone();
    let smtp_state_file = state.state_file.as_ref().clone();
    let smtp_spool_dir = state.spool_dir.as_ref().clone();
    let submission_state_file = state.state_file.as_ref().clone();
    let submission_core_base_url = {
        let snapshot = state.store.lock().unwrap().clone();
        snapshot.relay.core_delivery_base_url
    };
    let api_task = tokio::spawn(async move {
        let listener = TcpListener::bind(&bind_address).await?;
        info!("lpe-ct management api listening on http://{bind_address}");
        axum::serve(listener, router(api_state)).await?;
        Result::<()>::Ok(())
    });
    let reporting_state = state.clone();
    let reporting_task = tokio::spawn(async move {
        run_reporting_scheduler(reporting_state).await;
        Result::<()>::Ok(())
    });
    let smtp_task = tokio::spawn(async move {
        smtp::run_smtp_listener(smtp_bind_address, smtp_state_file, smtp_spool_dir).await
    });
    let submission_task = tokio::spawn(async move {
        if let Some(bind_address) = submission_bind_address {
            submission::run_submission_listener(
                bind_address,
                submission_core_base_url,
                submission_state_file,
            )
            .await?;
        } else {
            let _ = std::future::pending::<Result<()>>().await;
        }
        Result::<()>::Ok(())
    });

    tokio::select! {
        result = api_task => result??,
        result = reporting_task => result??,
        result = smtp_task => result??,
        result = submission_task => result??,
    }

    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/logout", post(logout))
        .route("/api/v1/auth/me", get(me))
        .route(
            "/metrics",
            get(|State(state): State<AppState>| async move {
                observability::metrics_endpoint(state.spool_dir.clone()).await
            }),
        )
        .route("/api/v1/dashboard", get(dashboard))
        .route("/api/v1/quarantine", get(quarantine_items))
        .route("/api/v1/history", get(mail_history))
        .route("/api/v1/history/{trace_id}", get(trace_history))
        .route("/api/v1/traces/{trace_id}", get(trace_details))
        .route("/api/v1/traces/{trace_id}/retry", post(retry_trace))
        .route("/api/v1/traces/{trace_id}/release", post(release_trace))
        .route("/api/v1/traces/{trace_id}/delete", post(delete_trace))
        .route("/api/v1/routes/diagnostics", get(route_diagnostics))
        .route("/api/v1/policies/status", get(policy_status))
        .route(
            "/api/v1/reporting",
            get(reporting_snapshot).put(update_reporting),
        )
        .route("/api/v1/reporting/digests/run", post(run_digest_reports))
        .route("/api/v1/reporting/digests", get(digest_reports))
        .route(
            "/api/v1/reporting/digests/{report_id}",
            get(digest_report_details),
        )
        .route("/api/v1/site", put(update_site))
        .route("/api/v1/relay", put(update_relay))
        .route("/api/v1/network", put(update_network))
        .route("/api/v1/policies", put(update_policies))
        .route("/api/v1/updates", put(update_updates))
        .route(
            "/api/v1/integration/outbound-messages",
            post(outbound_handoff),
        )
        .layer(middleware::from_fn(observability::observe_http))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    let snapshot = read_state(&state)?;
    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        service: "lpe-ct".to_string(),
        node_name: snapshot.site.node_name,
        role: snapshot.site.role,
    }))
}

async fn health_live(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    health(State(state)).await
}

async fn health_ready(State(state): State<AppState>) -> Result<Json<ReadinessResponse>, ApiError> {
    let snapshot = read_state(&state)?;
    let mut checks = Vec::new();

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

    checks.push(check_state_file(&state.state_file));
    checks.push(check_spool_layout(&state.spool_dir));
    checks.push(check_local_data_store_policy(&snapshot.local_data_stores));
    checks.push(check_non_empty_value(
        "primary-relay",
        true,
        &snapshot.relay.primary_upstream,
        "primary relay is configured",
        "primary relay is missing",
    ));
    checks.push(check_non_empty_value(
        "core-delivery-base-url",
        true,
        &snapshot.relay.core_delivery_base_url,
        "core delivery base URL is configured",
        "core delivery base URL is missing",
    ));
    checks.push(
        check_optional_http_dependency(
            "core-delivery-api",
            &format!(
                "{}/health/live",
                snapshot.relay.core_delivery_base_url.trim_end_matches('/')
            ),
            &format!(
                "core delivery API reachable at {}",
                snapshot.relay.core_delivery_base_url
            ),
            "core delivery API unreachable; inbound mail will remain queued locally until recovery",
        )
        .await,
    );
    checks.push(
        check_optional_tcp_dependency(
            "relay-reachability",
            &snapshot.relay.primary_upstream,
            "primary relay target accepted a TCP connection",
            "primary relay target is unreachable; outbound spool will grow until recovery",
        )
        .await,
    );
    checks.push(check_spool_pressure(&state.spool_dir));
    checks.push(check_quarantine_backlog(&state.spool_dir));

    Ok(Json(ReadinessResponse {
        status: readiness_status(&checks).to_string(),
        service: "lpe-ct".to_string(),
        node_name: snapshot.site.node_name,
        role: snapshot.site.role,
        warnings: checks.iter().filter(|check| check.status == "warn").count() as u32,
        checks,
    }))
}

async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    let snapshot = read_state(&state)?;
    let email = payload.email.trim().to_lowercase();
    if email != snapshot.management_auth.admin_email.trim().to_lowercase()
        || !verify_password(&snapshot.management_auth.password_hash, &payload.password)
    {
        observability::record_security_event("management_auth_failure");
        append_audit_event_with_actor(
            &state,
            &email,
            "management-login-failed",
            "Invalid LPE-CT management credentials",
        )?;
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid management credentials",
        ));
    }

    let token = Uuid::new_v4().to_string();
    state
        .sessions
        .lock()
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "session lock poisoned"))?
        .insert(
            token.clone(),
            ManagementSession {
                email: email.clone(),
                auth_method: "password".to_string(),
            },
        );
    append_audit_event_with_actor(
        &state,
        &email,
        "management-login-succeeded",
        "LPE-CT management session opened",
    )?;

    Ok(Json(LoginResponse {
        token,
        admin: ManagementIdentity {
            email,
            auth_method: "password".to_string(),
        },
    }))
}

async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<HealthResponse>, ApiError> {
    if let Some(token) = bearer_token(&headers) {
        if let Some(session) = state
            .sessions
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "session lock poisoned"))?
            .remove(&token)
        {
            append_audit_event_with_actor(
                &state,
                &session.email,
                "management-logout",
                "LPE-CT management session closed",
            )?;
        }
    }

    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        service: "lpe-ct".to_string(),
        node_name: "management".to_string(),
        role: "management".to_string(),
    }))
}

async fn me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ManagementIdentity>, ApiError> {
    let session = require_management_admin(&state, &headers)?;
    Ok(Json(ManagementIdentity {
        email: session.email,
        auth_method: session.auth_method,
    }))
}

async fn dashboard(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DashboardState>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let mut snapshot = read_state(&state)?;
    snapshot.queues = smtp::queue_metrics(&state.spool_dir, snapshot.queues.upstream_reachable)
        .map_err(ApiError::from)?;
    Ok(Json(snapshot))
}

async fn quarantine_items(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<smtp::QuarantineQuery>,
) -> Result<Json<Vec<smtp::QuarantineSummary>>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let runtime = {
        let snapshot = read_state(&state)?;
        smtp::runtime_config_from_dashboard(&snapshot)
    };
    let items = smtp::list_quarantine_items(&state.spool_dir, &runtime, query)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(items))
}

async fn mail_history(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Query<reporting::HistoryQuery>,
) -> Result<Json<reporting::MailHistoryResponse>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    let runtime = smtp::runtime_config_from_dashboard(&snapshot);
    let history = reporting::search_mail_history(
        &state.spool_dir,
        &runtime,
        query,
        snapshot.reporting.history_retention_days,
    )
    .await
    .map_err(ApiError::from)?;
    Ok(Json(history))
}

async fn trace_history(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<reporting::TraceHistoryDetails>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    let runtime = smtp::runtime_config_from_dashboard(&snapshot);
    let details = reporting::load_trace_history(
        &state.spool_dir,
        &runtime,
        &trace_id,
        snapshot.reporting.history_retention_days,
    )
    .await
    .map_err(ApiError::from)?;
    Ok(Json(details))
}

async fn trace_details(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<smtp::TraceDetails>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let details = smtp::load_trace_details(&state.spool_dir, &trace_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "trace not found"))?;
    Ok(Json(details))
}

async fn retry_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<smtp::TraceActionResult>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let runtime = {
        let snapshot = read_state(&state)?;
        smtp::runtime_config_from_dashboard(&snapshot)
    };
    let result = smtp::retry_trace(&state.spool_dir, &runtime, &trace_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "trace not found"))?;
    if result.to_queue.is_empty() {
        return Err(ApiError::new(StatusCode::CONFLICT, result.detail));
    }
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "trace-retry",
        &format!("requested retry for {}", result.trace_id),
    )?;
    Ok(Json(result))
}

async fn release_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<smtp::TraceActionResult>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let runtime = {
        let snapshot = read_state(&state)?;
        smtp::runtime_config_from_dashboard(&snapshot)
    };
    let result = smtp::release_trace(&state.spool_dir, &runtime, &trace_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "trace not found"))?;
    if result.to_queue.is_empty() {
        return Err(ApiError::new(StatusCode::CONFLICT, result.detail));
    }
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "trace-release",
        &format!("requested release for {}", result.trace_id),
    )?;
    Ok(Json(result))
}

async fn delete_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(trace_id): AxumPath<String>,
) -> Result<Json<smtp::TraceActionResult>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let runtime = {
        let snapshot = read_state(&state)?;
        smtp::runtime_config_from_dashboard(&snapshot)
    };
    let result = smtp::delete_trace(&state.spool_dir, &runtime, &trace_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "trace not found"))?;
    if result.to_queue.is_empty() {
        return Err(ApiError::new(StatusCode::CONFLICT, result.detail));
    }
    append_audit_event_with_actor(
        &state,
        &admin.email,
        "trace-delete",
        &format!("deleted quarantined trace {}", result.trace_id),
    )?;
    Ok(Json(result))
}

async fn route_diagnostics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<RouteDiagnosticsResponse>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    Ok(Json(RouteDiagnosticsResponse {
        primary_upstream: snapshot.relay.primary_upstream,
        secondary_upstream: snapshot.relay.secondary_upstream,
        routing: snapshot.routing,
        throttling: snapshot.throttling,
    }))
}

async fn policy_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PolicyStatusResponse>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    let runtime = smtp::runtime_config_from_dashboard(&snapshot);
    let recipient_verification_operational_state =
        if !snapshot.policies.recipient_verification.enabled {
            "disabled".to_string()
        } else if runtime.core_delivery_base_url.trim().is_empty() {
            "misconfigured".to_string()
        } else if snapshot.local_data_stores.dedicated_postgres.enabled
            && runtime.local_db.database_url.is_none()
        {
            "degraded".to_string()
        } else if integration_shared_secret().is_err() {
            "bridge-misconfigured".to_string()
        } else {
            "active".to_string()
        };
    let dkim_domains = snapshot
        .policies
        .dkim
        .domains
        .iter()
        .map(|domain| DkimDomainStatusView {
            domain: domain.domain.clone(),
            selector: domain.selector.clone(),
            private_key_path: domain.private_key_path.clone(),
            enabled: domain.enabled,
            key_status: dkim_key_status(&domain.private_key_path),
        })
        .collect::<Vec<_>>();
    let active_dkim_domains = dkim_domains
        .iter()
        .filter(|domain| domain.enabled && domain.key_status == "present")
        .count();
    Ok(Json(PolicyStatusResponse {
        recipient_verification: RecipientVerificationStatusView {
            enabled: snapshot.policies.recipient_verification.enabled,
            fail_closed: snapshot.policies.recipient_verification.fail_closed,
            cache_ttl_seconds: snapshot.policies.recipient_verification.cache_ttl_seconds,
            operational_state: recipient_verification_operational_state,
            cache_backend: if snapshot.local_data_stores.dedicated_postgres.enabled
                && runtime.local_db.database_url.is_some()
            {
                "private-postgres".to_string()
            } else if snapshot.local_data_stores.dedicated_postgres.enabled {
                "misconfigured-private-postgres".to_string()
            } else {
                "memory-only".to_string()
            },
        },
        dkim: DkimStatusView {
            enabled: snapshot.policies.dkim.enabled,
            operational_state: if !snapshot.policies.dkim.enabled {
                "disabled".to_string()
            } else if active_dkim_domains == 0 {
                "misconfigured".to_string()
            } else {
                "active".to_string()
            },
            headers: snapshot.policies.dkim.headers.clone(),
            over_sign: snapshot.policies.dkim.over_sign,
            expiration_seconds: snapshot.policies.dkim.expiration_seconds,
            domains: dkim_domains,
        },
    }))
}

async fn update_site(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SiteProfile>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    mutate_state(&state, &admin.email, "update-site", move |dashboard| {
        dashboard.site = payload;
    })
}

async fn update_relay(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<RelaySettings>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    mutate_state(&state, &admin.email, "update-relay", move |dashboard| {
        dashboard.relay = payload;
    })
}

async fn update_network(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<NetworkSettings>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    mutate_state(&state, &admin.email, "update-network", move |dashboard| {
        dashboard.network = payload;
    })
}

async fn update_policies(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut payload): Json<PolicySettings>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    normalize_policy_settings(&mut payload);
    let previous = read_state(&state)?;
    let response = mutate_state(&state, &admin.email, "update-policies", move |dashboard| {
        dashboard.policies = payload;
    })?;
    if let Err(error) = sync_technical_store(&state).await {
        restore_dashboard_state(&state, &previous)?;
        return Err(error);
    }
    Ok(response)
}

async fn update_updates(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<UpdateSettings>,
) -> Result<Json<DashboardState>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    mutate_state(&state, &admin.email, "update-updates", move |dashboard| {
        dashboard.updates = payload;
    })
}

async fn reporting_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<reporting::ReportingSnapshot>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let snapshot = read_state(&state)?;
    let reporting =
        reporting::snapshot(&state.spool_dir, &snapshot.reporting).map_err(ApiError::from)?;
    Ok(Json(reporting))
}

async fn update_reporting(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut payload): Json<reporting::ReportingSettings>,
) -> Result<Json<reporting::ReportingSnapshot>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    reporting::normalize_reporting_settings(&mut payload);
    let previous = read_state(&state)?;
    {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        guard.reporting = payload;
        append_dashboard_audit_event(&mut guard, &admin.email, "update-reporting");
        persist_state(&state.state_file, &guard)?;
    }
    if let Err(error) = sync_technical_store(&state).await {
        restore_dashboard_state(&state, &previous)?;
        return Err(error);
    }
    let snapshot = read_state(&state)?;
    let reporting =
        reporting::snapshot(&state.spool_dir, &snapshot.reporting).map_err(ApiError::from)?;
    Ok(Json(reporting))
}

async fn run_digest_reports(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<reporting::DigestRunResponse>, ApiError> {
    let admin = require_management_admin(&state, &headers)?;
    let generated_at = current_timestamp();
    let generated_reports = {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        let generated_reports =
            reporting::run_digest_generation(&state.spool_dir, &mut guard.reporting)
                .map_err(ApiError::from)?;
        guard.audit.insert(
            0,
            AuditEvent {
                timestamp: generated_at.clone(),
                actor: admin.email.clone(),
                action: "run-quarantine-digests".to_string(),
                details: format!(
                    "generated {} quarantine digest report(s)",
                    generated_reports.len()
                ),
            },
        );
        guard.audit.truncate(12);
        persist_state(&state.state_file, &guard)?;
        generated_reports
    };
    let next_digest_run_at = read_state(&state)?.reporting.next_digest_run_at;
    Ok(Json(reporting::DigestRunResponse {
        generated_at,
        generated_reports,
        next_digest_run_at,
    }))
}

async fn digest_reports(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<reporting::DigestReportSummary>>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let reports =
        reporting::list_recent_digest_reports(&state.spool_dir, 20).map_err(ApiError::from)?;
    Ok(Json(reports))
}

async fn digest_report_details(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(report_id): AxumPath<String>,
) -> Result<Json<reporting::DigestReportDetails>, ApiError> {
    let _admin = require_management_admin(&state, &headers)?;
    let report = reporting::load_digest_report(&state.spool_dir, &report_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "digest report not found"))?;
    Ok(Json(report))
}

async fn outbound_handoff(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<OutboundMessageHandoffRequest>,
) -> Result<Json<OutboundMessageHandoffResponse>, ApiError> {
    let request_trace_id = observability::trace_id_from_headers(&headers);
    let queue_id = payload.queue_id;
    let message_id = payload.message_id;
    let internet_message_id = payload.internet_message_id.clone();
    require_integration_request(&headers, OUTBOUND_HANDOFF_PATH, &payload)?;
    if let Some(role) = ha_non_active_role_for_traffic().map_err(ApiError::from)? {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("node role {role} does not accept outbound handoff traffic"),
        ));
    }
    let snapshot = read_state(&state)?;
    let runtime = smtp::runtime_config_from_dashboard(&snapshot);
    let response = smtp::process_outbound_handoff(&state.spool_dir, &runtime, payload)
        .await
        .map_err(ApiError::from)?;
    observability::record_outbound_handoff(response.status.as_str());
    info!(
        trace_id = %response.trace_id,
        upstream_trace_id = %request_trace_id,
        queue_id = %queue_id,
        message_id = %message_id,
        status = response.status.as_str(),
        internet_message_id = internet_message_id.as_deref().unwrap_or(""),
        "outbound handoff processed"
    );
    Ok(Json(response))
}

fn mutate_state<F>(
    state: &AppState,
    actor: &str,
    action: &str,
    update: F,
) -> Result<Json<DashboardState>, ApiError>
where
    F: FnOnce(&mut DashboardState),
{
    let mut guard = state
        .store
        .lock()
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
    update(&mut guard);
    append_dashboard_audit_event(&mut guard, actor, action);
    persist_state(&state.state_file, &guard)?;
    Ok(Json(guard.clone()))
}

async fn run_reporting_scheduler(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        {
            let snapshot = match state.store.lock() {
                Ok(guard) => guard.clone(),
                Err(_) => continue,
            };
            let runtime = smtp::runtime_config_from_dashboard(&snapshot);
            if let Err(error) =
                reporting::enforce_retention(&state.spool_dir, &runtime, &snapshot.reporting).await
            {
                tracing::warn!(error = %error, "reporting retention enforcement failed");
            }
        }
        let generated_reports = {
            let mut guard = match state.store.lock() {
                Ok(guard) => guard,
                Err(_) => continue,
            };
            let generated_reports = match reporting::run_due_digest_generation(
                &state.spool_dir,
                &mut guard.reporting,
            ) {
                Ok(generated_reports) => generated_reports,
                Err(error) => {
                    tracing::warn!(error = %error, "scheduled digest generation failed");
                    continue;
                }
            };
            if generated_reports.is_empty() {
                0
            } else {
                guard.audit.insert(
                    0,
                    AuditEvent {
                        timestamp: current_timestamp(),
                        actor: "system".to_string(),
                        action: "scheduled-quarantine-digests".to_string(),
                        details: format!(
                            "generated {} scheduled quarantine digest report(s)",
                            generated_reports.len()
                        ),
                    },
                );
                guard.audit.truncate(12);
                if let Err(error) = persist_state(&state.state_file, &guard) {
                    tracing::warn!(error = %error, "unable to persist scheduled reporting state");
                }
                generated_reports.len()
            }
        };
        if generated_reports > 0 {
            tracing::info!(generated_reports, "scheduled quarantine digests generated");
        }
    }
}

fn read_state(state: &AppState) -> Result<DashboardState, ApiError> {
    state
        .store
        .lock()
        .map(|guard| guard.clone())
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))
}

fn local_db_config_from_dashboard(dashboard: &DashboardState) -> storage::LocalDbConfig {
    storage::LocalDbConfig {
        enabled: dashboard.local_data_stores.dedicated_postgres.enabled,
        database_url: env::var("LPE_CT_LOCAL_DB_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
    }
}

async fn sync_technical_store(state: &AppState) -> Result<(), ApiError> {
    let snapshot = read_state(state)?;
    storage::sync_dashboard_configuration(&local_db_config_from_dashboard(&snapshot), &snapshot)
        .await
        .map_err(ApiError::from)
}

fn restore_dashboard_state(state: &AppState, snapshot: &DashboardState) -> Result<(), ApiError> {
    {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        *guard = snapshot.clone();
    }
    persist_state(&state.state_file, snapshot).map_err(ApiError::from)
}

fn append_dashboard_audit_event(state: &mut DashboardState, actor: &str, action: &str) {
    state.audit.insert(
        0,
        AuditEvent {
            timestamp: current_timestamp(),
            actor: actor.to_string(),
            action: action.to_string(),
            details: "DMZ sorting center configuration updated".to_string(),
        },
    );
    state.audit.truncate(12);
}

fn append_audit_event_with_actor(
    state: &AppState,
    actor: &str,
    action: &str,
    details: &str,
) -> Result<(), ApiError> {
    let mut guard = state
        .store
        .lock()
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
    guard.audit.insert(
        0,
        AuditEvent {
            timestamp: current_timestamp(),
            actor: actor.to_string(),
            action: action.to_string(),
            details: details.to_string(),
        },
    );
    guard.audit.truncate(12);
    persist_state(&state.state_file, &guard)?;
    Ok(())
}

fn load_or_initialize_state(path: &Path) -> Result<DashboardState> {
    if path.exists() {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("unable to read state file {}", path.display()))?;
        let state = serde_json::from_str::<DashboardState>(&raw)
            .with_context(|| format!("unable to parse state file {}", path.display()))?;
        return Ok(state);
    }

    let state = default_state();
    persist_state(path, &state)?;
    Ok(state)
}

fn persist_state(path: &Path, state: &DashboardState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("unable to create state directory {}", parent.display()))?;
    }

    let serialized = serde_json::to_string_pretty(state)?;
    fs::write(path, serialized)
        .with_context(|| format!("unable to write state file {}", path.display()))?;
    Ok(())
}

fn apply_env_overrides(state: &mut DashboardState) {
    if let Ok(value) = env::var("LPE_CT_NODE_NAME") {
        state.site.node_name = value;
    }
    if let Ok(value) = env::var("LPE_CT_USE_HA") {
        state.relay.ha_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_RELAY_PRIMARY") {
        state.relay.primary_upstream = value;
    }
    if let Ok(value) = env::var("LPE_CT_RELAY_SECONDARY") {
        state.relay.secondary_upstream = value;
    }
    if let Ok(value) = env::var("LPE_CT_CORE_DELIVERY_BASE_URL") {
        state.relay.core_delivery_base_url = value;
    }
    if let Ok(value) = env::var("LPE_CT_MUTUAL_TLS_REQUIRED") {
        state.relay.mutual_tls_required = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_FALLBACK_TO_HOLD_QUEUE") {
        state.relay.fallback_to_hold_queue = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_DRAIN_MODE") {
        state.policies.drain_mode = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_GREYLISTING_ENABLED") {
        state.policies.greylisting_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ANTIVIRUS_ENABLED") {
        state.policies.antivirus_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ANTIVIRUS_FAIL_CLOSED") {
        state.policies.antivirus_fail_closed = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ANTIVIRUS_PROVIDER_CHAIN") {
        state.policies.antivirus_provider_chain = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_ENABLED") {
        state.policies.bayespam_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_AUTO_LEARN") {
        state.policies.bayespam_auto_learn = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_SCORE_WEIGHT") {
        if let Ok(parsed) = value.parse::<f32>() {
            state.policies.bayespam_score_weight = parsed.max(0.0);
        }
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_MIN_TOKEN_LENGTH") {
        if let Ok(parsed) = value.parse::<u32>() {
            state.policies.bayespam_min_token_length = parsed.max(2);
        }
    }
    if let Ok(value) = env::var("LPE_CT_BAYESPAM_MAX_TOKENS") {
        if let Ok(parsed) = value.parse::<u32>() {
            state.policies.bayespam_max_tokens = parsed.max(16);
        }
    }
    if let Ok(value) = env::var("LPE_CT_REQUIRE_SPF") {
        state.policies.require_spf = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_REQUIRE_DKIM_ALIGNMENT") {
        state.policies.require_dkim_alignment = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_REQUIRE_DMARC_ENFORCEMENT") {
        state.policies.require_dmarc_enforcement = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_DEFER_ON_AUTH_TEMPFAIL") {
        state.policies.defer_on_auth_tempfail = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_DNSBL_ENABLED") {
        state.policies.dnsbl_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_DNSBL_ZONES") {
        state.policies.dnsbl_zones = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_REPUTATION_ENABLED") {
        state.policies.reputation_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_REPUTATION_QUARANTINE_THRESHOLD") {
        if let Ok(parsed) = value.parse::<i32>() {
            state.policies.reputation_quarantine_threshold = parsed;
        }
    }
    if let Ok(value) = env::var("LPE_CT_REPUTATION_REJECT_THRESHOLD") {
        if let Ok(parsed) = value.parse::<i32>() {
            state.policies.reputation_reject_threshold = parsed;
        }
    }
    if let Ok(value) = env::var("LPE_CT_SPAM_QUARANTINE_THRESHOLD") {
        if let Ok(parsed) = value.parse::<f32>() {
            state.policies.spam_quarantine_threshold = parsed.max(0.0);
        }
    }
    if let Ok(value) = env::var("LPE_CT_SPAM_REJECT_THRESHOLD") {
        if let Ok(parsed) = value.parse::<f32>() {
            state.policies.spam_reject_threshold =
                parsed.max(state.policies.spam_quarantine_threshold);
        }
    }
    if let Ok(value) = env::var("LPE_CT_MAX_MESSAGE_SIZE_MB") {
        if let Ok(parsed) = value.parse::<u32>() {
            state.policies.max_message_size_mb = parsed.max(1);
        }
    }
    if let Ok(value) = env::var("LPE_CT_POLICY_ALLOW_SENDERS") {
        state.policies.address_policy.allow_senders = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_POLICY_BLOCK_SENDERS") {
        state.policies.address_policy.block_senders = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_POLICY_ALLOW_RECIPIENTS") {
        state.policies.address_policy.allow_recipients = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_POLICY_BLOCK_RECIPIENTS") {
        state.policies.address_policy.block_recipients = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_RECIPIENT_VERIFICATION_ENABLED") {
        state.policies.recipient_verification.enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_RECIPIENT_VERIFICATION_FAIL_CLOSED") {
        state.policies.recipient_verification.fail_closed = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_RECIPIENT_VERIFICATION_CACHE_TTL_SECONDS") {
        if let Ok(parsed) = value.parse::<u32>() {
            state.policies.recipient_verification.cache_ttl_seconds = parsed.max(1);
        }
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_ALLOW_EXTENSIONS") {
        state.policies.attachment_policy.allow_extensions = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_BLOCK_EXTENSIONS") {
        state.policies.attachment_policy.block_extensions = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_ALLOW_MIME_TYPES") {
        state.policies.attachment_policy.allow_mime_types = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_BLOCK_MIME_TYPES") {
        state.policies.attachment_policy.block_mime_types = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_ALLOW_DETECTED_TYPES") {
        state.policies.attachment_policy.allow_detected_types = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_ATTACHMENT_BLOCK_DETECTED_TYPES") {
        state.policies.attachment_policy.block_detected_types = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_ENABLED") {
        state.policies.dkim.enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_HEADERS") {
        state.policies.dkim.headers = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_OVER_SIGN") {
        state.policies.dkim.over_sign = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_EXPIRATION_SECONDS") {
        state.policies.dkim.expiration_seconds =
            value.parse::<u32>().ok().filter(|value| *value > 0);
    }
    if let Ok(value) = env::var("LPE_CT_OUTBOUND_DKIM_KEYS") {
        let domains = value
            .split(';')
            .filter_map(|entry| {
                let trimmed = entry.trim();
                if trimmed.is_empty() {
                    return None;
                }
                let mut parts = trimmed.split('|').map(str::trim);
                Some(DkimDomainConfig {
                    domain: parts.next()?.to_ascii_lowercase(),
                    selector: parts.next()?.to_string(),
                    private_key_path: parts.next()?.to_string(),
                    enabled: true,
                })
            })
            .collect::<Vec<_>>();
        if !domains.is_empty() {
            state.policies.dkim.domains = domains;
        }
    }
    state.local_data_stores.state_file_path = env::var("LPE_CT_STATE_FILE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| state.local_data_stores.state_file_path.clone());
    state.local_data_stores.spool_root = env::var("LPE_CT_SPOOL_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| state.local_data_stores.spool_root.clone());
    if let Ok(value) = env::var("LPE_CT_LOCAL_DB_ENABLED") {
        state.local_data_stores.dedicated_postgres.enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_LOCAL_DB_LISTEN_ADDRESS") {
        let trimmed = value.trim();
        state.local_data_stores.dedicated_postgres.listen_address = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
    }
    if let Ok(value) = env::var("LPE_CT_LOCAL_DB_NETWORK_SCOPE") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            state.local_data_stores.dedicated_postgres.network_scope = trimmed.to_string();
        }
    }
    if let Ok(value) = env::var("LPE_CT_LOCAL_DB_PURPOSES") {
        let parsed = parse_csv(&value);
        if !parsed.is_empty() {
            state.local_data_stores.dedicated_postgres.purposes = parsed;
        }
    }
}

fn normalize_policy_settings(policies: &mut PolicySettings) {
    let mut antivirus_provider_chain = policies
        .antivirus_provider_chain
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let mut seen = std::collections::BTreeSet::new();
    antivirus_provider_chain.retain(|value| seen.insert(value.clone()));
    if policies.antivirus_enabled && antivirus_provider_chain.is_empty() {
        antivirus_provider_chain = default_antivirus_provider_chain();
    }
    policies.antivirus_provider_chain = antivirus_provider_chain;
    if policies.bayespam_min_token_length < 2 {
        policies.bayespam_min_token_length = 2;
    }
    if policies.bayespam_max_tokens < 16 {
        policies.bayespam_max_tokens = 16;
    }
    if policies.bayespam_score_weight < 0.0 {
        policies.bayespam_score_weight = 0.0;
    }
    if policies.reputation_reject_threshold > policies.reputation_quarantine_threshold {
        policies.reputation_reject_threshold = policies.reputation_quarantine_threshold;
    }
    if policies.spam_reject_threshold < policies.spam_quarantine_threshold {
        policies.spam_reject_threshold = policies.spam_quarantine_threshold;
    }
    normalize_csv_rules(&mut policies.address_policy.allow_senders);
    normalize_csv_rules(&mut policies.address_policy.block_senders);
    normalize_csv_rules(&mut policies.address_policy.allow_recipients);
    normalize_csv_rules(&mut policies.address_policy.block_recipients);
    policies.recipient_verification.cache_ttl_seconds =
        policies.recipient_verification.cache_ttl_seconds.max(1);
    normalize_attachment_extension_rules(&mut policies.attachment_policy.allow_extensions);
    normalize_attachment_extension_rules(&mut policies.attachment_policy.block_extensions);
    normalize_csv_rules(&mut policies.attachment_policy.allow_mime_types);
    normalize_csv_rules(&mut policies.attachment_policy.block_mime_types);
    normalize_csv_rules(&mut policies.attachment_policy.allow_detected_types);
    normalize_csv_rules(&mut policies.attachment_policy.block_detected_types);
    normalize_csv_rules(&mut policies.dkim.headers);
    if policies.dkim.headers.is_empty() {
        policies.dkim.headers = default_dkim_headers();
    }
    if policies.dkim.headers.iter().all(|value| value != "sender") {
        policies.dkim.headers.push("sender".to_string());
    }
    let mut seen_domains = std::collections::BTreeSet::new();
    policies.dkim.domains.retain_mut(|domain| {
        domain.domain = domain.domain.trim().to_ascii_lowercase();
        domain.selector = domain.selector.trim().to_string();
        domain.private_key_path = domain.private_key_path.trim().to_string();
        !domain.domain.is_empty()
            && !domain.selector.is_empty()
            && !domain.private_key_path.is_empty()
            && seen_domains.insert(domain.domain.clone())
    });
}

fn normalize_local_data_stores(local_data_stores: &mut LocalDataStoresSettings) {
    if local_data_stores.state_file_path.trim().is_empty() {
        local_data_stores.state_file_path = "/var/lib/lpe-ct/state.json".to_string();
    }
    if local_data_stores.spool_root.trim().is_empty() {
        local_data_stores.spool_root = "/var/spool/lpe-ct".to_string();
    }
    if local_data_stores.spool_queues.is_empty() {
        local_data_stores.spool_queues = default_spool_queues();
    }
    if local_data_stores.policy_artifacts.is_empty() {
        local_data_stores.policy_artifacts = default_policy_artifacts();
    }
    if local_data_stores.forbidden_canonical_data.is_empty() {
        local_data_stores.forbidden_canonical_data = default_forbidden_canonical_data();
    }
    local_data_stores.dedicated_postgres.network_scope =
        normalize_local_db_network_scope(&local_data_stores.dedicated_postgres.network_scope);
    if local_data_stores.dedicated_postgres.enabled
        && local_data_stores
            .dedicated_postgres
            .listen_address
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        local_data_stores.dedicated_postgres.listen_address =
            Some(default_local_db_listen_address());
    }
    if local_data_stores.dedicated_postgres.purposes.is_empty() {
        local_data_stores.dedicated_postgres.purposes = default_local_db_purposes();
    }
    local_data_stores.dedicated_postgres.purposes.sort();
    local_data_stores.dedicated_postgres.purposes.dedup();
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn normalize_csv_rules(values: &mut Vec<String>) {
    let mut seen = std::collections::BTreeSet::new();
    *values = values
        .iter()
        .map(|value| value.trim().trim_start_matches('@').to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .filter(|value| seen.insert(value.clone()))
        .collect();
}

fn normalize_attachment_extension_rules(values: &mut Vec<String>) {
    let mut seen = std::collections::BTreeSet::new();
    *values = values
        .iter()
        .map(|value| value.trim().trim_start_matches('.').to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .filter(|value| seen.insert(value.clone()))
        .collect();
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().trim_matches('_').to_string())
        .filter(|value| !value.is_empty())
}

fn required_trimmed_env(name: &str) -> Result<String> {
    env_value(name).ok_or_else(|| anyhow::anyhow!("{name} must be set"))
}

fn local_hostname() -> String {
    env_value("HOSTNAME")
        .or_else(|| env_value("COMPUTERNAME"))
        .unwrap_or_else(|| "localhost".to_string())
}

fn ensure_management_bootstrap(state: &mut DashboardState) -> Result<()> {
    if state.management_auth.admin_email.trim().is_empty()
        || state.management_auth.password_hash.trim().is_empty()
    {
        let admin_email = required_trimmed_env("LPE_CT_BOOTSTRAP_ADMIN_EMAIL")?.to_lowercase();
        let password = required_trimmed_env("LPE_CT_BOOTSTRAP_ADMIN_PASSWORD")?;
        if is_known_weak_secret(password.trim()) {
            anyhow::bail!(
                "LPE_CT_BOOTSTRAP_ADMIN_PASSWORD uses a forbidden weak placeholder value"
            );
        }
        state.management_auth = ManagementAuthState {
            admin_email: admin_email.clone(),
            password_hash: hash_password(password.trim())?,
        };
        state.audit.insert(
            0,
            AuditEvent {
                timestamp: current_timestamp(),
                actor: "system".to_string(),
                action: "seed-management-admin".to_string(),
                details: format!("Bootstrap LPE-CT management admin prepared for {admin_email}"),
            },
        );
        state.audit.truncate(12);
    }

    Ok(())
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn default_state() -> DashboardState {
    let node_name = env_value("LPE_CT_NODE_NAME")
        .or_else(|| env_value("LPE_CT_SERVER_NAME"))
        .unwrap_or_else(local_hostname);
    let management_fqdn = env_value("LPE_CT_MANAGEMENT_FQDN")
        .or_else(|| env_value("LPE_CT_SERVER_NAME"))
        .unwrap_or_else(|| node_name.clone());
    let published_mx = env_value("LPE_CT_PUBLISHED_MX").unwrap_or_else(|| management_fqdn.clone());
    let primary_upstream = env_value("LPE_CT_RELAY_PRIMARY").unwrap_or_default();
    let secondary_upstream = env_value("LPE_CT_RELAY_SECONDARY").unwrap_or_default();
    let outbound_smart_hosts = [primary_upstream.clone(), secondary_upstream.clone()]
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim_start_matches("smtp://").to_string())
        .collect();
    let routing_rules = if primary_upstream.trim().is_empty() {
        Vec::new()
    } else {
        vec![RoutingRule {
            id: "default-primary".to_string(),
            description: "Default outbound route through the configured primary relay".to_string(),
            sender_domain: None,
            recipient_domain: None,
            relay_target: primary_upstream.clone(),
        }]
    };
    DashboardState {
        site: SiteProfile {
            node_name,
            role: "dmz-sorting-center".to_string(),
            region: env_value("LPE_CT_REGION").unwrap_or_default(),
            dmz_zone: env_value("LPE_CT_DMZ_ZONE").unwrap_or_default(),
            published_mx,
            management_fqdn,
            public_smtp_bind: env_value("LPE_CT_SMTP_BIND_ADDRESS")
                .unwrap_or_else(|| "0.0.0.0:25".to_string()),
            management_bind: env_value("LPE_CT_BIND_ADDRESS")
                .unwrap_or_else(|| "127.0.0.1:8380".to_string()),
        },
        relay: RelaySettings {
            ha_enabled: env_value("LPE_CT_USE_HA")
                .map(|value| parse_bool(&value))
                .unwrap_or(true),
            primary_upstream,
            secondary_upstream,
            core_delivery_base_url: default_core_delivery_base_url(),
            mutual_tls_required: false,
            fallback_to_hold_queue: false,
            sync_interval_seconds: 30,
            lan_dependency_note: "Only relay and management flows to the LAN are allowed."
                .to_string(),
        },
        routing: RoutingSettings {
            rules: routing_rules,
        },
        throttling: ThrottlingSettings {
            enabled: true,
            rules: vec![ThrottleRule {
                id: "per-recipient-domain".to_string(),
                scope: "recipient-domain".to_string(),
                recipient_domain: None,
                sender_domain: None,
                max_messages: 20,
                window_seconds: 60,
                retry_after_seconds: 120,
            }],
        },
        network: NetworkSettings {
            allowed_management_cidrs: env_value("LPE_CT_ALLOWED_MANAGEMENT_CIDRS")
                .map(|value| parse_csv(&value))
                .unwrap_or_default(),
            allowed_upstream_cidrs: env_value("LPE_CT_ALLOWED_UPSTREAM_CIDRS")
                .map(|value| parse_csv(&value))
                .unwrap_or_default(),
            outbound_smart_hosts,
            public_listener_enabled: true,
            submission_listener_enabled: false,
            proxy_protocol_enabled: false,
            max_concurrent_sessions: 250,
        },
        local_data_stores: LocalDataStoresSettings {
            state_file_path: env_value("LPE_CT_STATE_FILE")
                .unwrap_or_else(|| "/var/lib/lpe-ct/state.json".to_string()),
            spool_root: env_value("LPE_CT_SPOOL_DIR")
                .unwrap_or_else(|| "/var/spool/lpe-ct".to_string()),
            spool_queues: default_spool_queues(),
            policy_artifacts: default_policy_artifacts(),
            forbidden_canonical_data: default_forbidden_canonical_data(),
            dedicated_postgres: LocalPostgresStore {
                enabled: true,
                purposes: default_local_db_purposes(),
                listen_address: Some(default_local_db_listen_address()),
                network_scope: default_local_db_network_scope(),
                public_exposure_forbidden: true,
                notes: default_local_db_notes(),
            },
        },
        policies: PolicySettings {
            drain_mode: false,
            quarantine_enabled: true,
            greylisting_enabled: true,
            antivirus_enabled: default_antivirus_enabled(),
            antivirus_fail_closed: default_antivirus_fail_closed(),
            antivirus_provider_chain: default_antivirus_provider_chain(),
            bayespam_enabled: default_bayespam_enabled(),
            bayespam_auto_learn: default_bayespam_auto_learn(),
            bayespam_score_weight: default_bayespam_score_weight(),
            bayespam_min_token_length: default_bayespam_min_token_length(),
            bayespam_max_tokens: default_bayespam_max_tokens(),
            require_spf: true,
            require_dkim_alignment: false,
            require_dmarc_enforcement: true,
            defer_on_auth_tempfail: default_defer_on_auth_tempfail(),
            dnsbl_enabled: default_dnsbl_enabled(),
            dnsbl_zones: default_dnsbl_zones(),
            reputation_enabled: default_reputation_enabled(),
            reputation_quarantine_threshold: default_reputation_quarantine_threshold(),
            reputation_reject_threshold: default_reputation_reject_threshold(),
            spam_quarantine_threshold: default_spam_quarantine_threshold(),
            spam_reject_threshold: default_spam_reject_threshold(),
            attachment_text_scan_enabled: true,
            max_message_size_mb: 64,
            address_policy: AddressPolicySettings {
                allow_senders: Vec::new(),
                block_senders: Vec::new(),
                allow_recipients: Vec::new(),
                block_recipients: Vec::new(),
            },
            recipient_verification: default_recipient_verification_settings(),
            attachment_policy: AttachmentPolicySettings {
                allow_extensions: Vec::new(),
                block_extensions: Vec::new(),
                allow_mime_types: Vec::new(),
                block_mime_types: Vec::new(),
                allow_detected_types: Vec::new(),
                block_detected_types: Vec::new(),
            },
            dkim: default_dkim_settings(),
        },
        reporting: reporting::default_reporting_settings(),
        updates: UpdateSettings {
            channel: "stable".to_string(),
            auto_download: false,
            maintenance_window: "Sun 02:30".to_string(),
            last_applied_release: "bootstrap".to_string(),
            update_source: env_value("LPE_CT_UPDATE_SOURCE")
                .unwrap_or_else(|| "git checkout".to_string()),
        },
        queues: QueueMetrics {
            inbound_messages: 0,
            deferred_messages: 0,
            quarantined_messages: 0,
            held_messages: 0,
            delivery_attempts_last_hour: 0,
            upstream_reachable: true,
        },
        management_auth: ManagementAuthState {
            admin_email: String::new(),
            password_hash: String::new(),
        },
        audit: Vec::new(),
    }
}

fn default_core_delivery_base_url() -> String {
    env_value("LPE_CT_CORE_DELIVERY_BASE_URL")
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string())
}

fn default_recipient_verification_cache_ttl_seconds() -> u32 {
    300
}

fn default_recipient_verification_settings() -> RecipientVerificationSettings {
    RecipientVerificationSettings {
        enabled: false,
        fail_closed: true,
        cache_ttl_seconds: default_recipient_verification_cache_ttl_seconds(),
    }
}

fn default_dkim_headers() -> Vec<String> {
    vec![
        "from".to_string(),
        "sender".to_string(),
        "to".to_string(),
        "cc".to_string(),
        "subject".to_string(),
        "mime-version".to_string(),
        "content-type".to_string(),
        "message-id".to_string(),
    ]
}

fn default_dkim_settings() -> DkimSettings {
    DkimSettings {
        enabled: false,
        headers: default_dkim_headers(),
        over_sign: true,
        expiration_seconds: Some(3600),
        domains: Vec::new(),
    }
}

fn submission_listener_is_configured(bind_address: &Option<String>) -> bool {
    bind_address
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
        && env::var("LPE_CT_SUBMISSION_TLS_CERT_PATH")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
        && env::var("LPE_CT_SUBMISSION_TLS_KEY_PATH")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
}

fn default_true() -> bool {
    true
}

fn default_spool_queues() -> Vec<String> {
    smtp::SPOOL_QUEUES
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

fn default_policy_artifacts() -> Vec<String> {
    smtp::POLICY_ARTIFACTS
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

fn default_forbidden_canonical_data() -> Vec<String> {
    [
        "mailboxes",
        "inbox",
        "sent",
        "drafts",
        "outbox",
        "contacts",
        "calendars",
        "tasks",
        "rights",
        "tenant-administration",
        "canonical-search",
        "bcc-business-storage",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn default_local_db_purposes() -> Vec<String> {
    [
        "bayesian",
        "reputation",
        "greylisting",
        "quarantine-metadata",
        "cluster-coordination",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn default_local_db_network_scope() -> String {
    "host-local".to_string()
}

fn default_local_db_listen_address() -> String {
    "127.0.0.1:5432".to_string()
}

fn default_local_db_notes() -> String {
    "Dedicated LPE-CT PostgreSQL is the default technical state store and may hold only perimeter-owned technical state."
        .to_string()
}

fn normalize_local_db_network_scope(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "host-local" | "private-backend" | "lpe-ct-cluster" => value.trim().to_ascii_lowercase(),
        _ => default_local_db_network_scope(),
    }
}

fn default_dnsbl_enabled() -> bool {
    true
}

fn default_antivirus_enabled() -> bool {
    false
}

fn default_antivirus_fail_closed() -> bool {
    true
}

fn default_antivirus_provider_chain() -> Vec<String> {
    vec!["takeri".to_string()]
}

fn default_bayespam_enabled() -> bool {
    true
}

fn default_bayespam_auto_learn() -> bool {
    true
}

fn default_bayespam_score_weight() -> f32 {
    6.0
}

fn default_bayespam_min_token_length() -> u32 {
    3
}

fn default_bayespam_max_tokens() -> u32 {
    256
}

fn default_defer_on_auth_tempfail() -> bool {
    true
}

fn default_dnsbl_zones() -> Vec<String> {
    vec!["zen.spamhaus.org".to_string(), "bl.spamcop.net".to_string()]
}

fn default_reputation_enabled() -> bool {
    true
}

fn default_reputation_quarantine_threshold() -> i32 {
    -4
}

fn default_reputation_reject_threshold() -> i32 {
    -8
}

fn default_spam_quarantine_threshold() -> f32 {
    5.0
}

fn default_spam_reject_threshold() -> f32 {
    9.0
}

fn current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format!("unix:{}", duration.as_secs()),
        Err(_) => "unix:0".to_string(),
    }
}

fn ha_role_file() -> Option<PathBuf> {
    env::var("LPE_CT_HA_ROLE_FILE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn read_ha_role() -> Result<Option<String>> {
    let Some(path) = ha_role_file() else {
        return Ok(None);
    };

    let role =
        fs::read_to_string(&path).with_context(|| format!("unable to read {}", path.display()))?;
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

pub(crate) fn ha_non_active_role_for_traffic() -> Result<Option<String>> {
    Ok(match read_ha_role()? {
        Some(role) if role != "active" => Some(role),
        _ => None,
    })
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

fn readiness_status(checks: &[ReadinessCheck]) -> &'static str {
    if checks
        .iter()
        .any(|check| check.critical && check.status == "failed")
    {
        "failed"
    } else {
        "ready"
    }
}

fn check_non_empty_value(
    name: &str,
    critical: bool,
    value: &str,
    ok_detail: &str,
    failed_detail: &str,
) -> ReadinessCheck {
    if value.trim().is_empty() {
        readiness_failed(name, critical, failed_detail)
    } else {
        readiness_ok(name, critical, ok_detail)
    }
}

fn check_state_file(path: &Path) -> ReadinessCheck {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => readiness_ok(
            "state-file",
            true,
            format!("state file is present at {}", path.display()),
        ),
        Ok(_) => readiness_failed(
            "state-file",
            true,
            format!("state path is not a regular file: {}", path.display()),
        ),
        Err(error) => readiness_failed(
            "state-file",
            true,
            format!("unable to access state file {}: {error}", path.display()),
        ),
    }
}

fn check_spool_layout(path: &Path) -> ReadinessCheck {
    let required = smtp::SPOOL_QUEUES;
    let missing = required
        .iter()
        .map(|entry| path.join(entry))
        .filter(|entry| !entry.is_dir())
        .map(|entry| entry.display().to_string())
        .collect::<Vec<_>>();

    if missing.is_empty() {
        readiness_ok(
            "spool-layout",
            true,
            format!("required spool directories exist under {}", path.display()),
        )
    } else {
        readiness_failed(
            "spool-layout",
            true,
            format!("missing spool directories: {}", missing.join(", ")),
        )
    }
}

fn check_local_data_store_policy(local_data_stores: &LocalDataStoresSettings) -> ReadinessCheck {
    let dedicated_postgres = &local_data_stores.dedicated_postgres;
    if !dedicated_postgres.enabled {
        return readiness_ok(
            "local-data-stores",
            true,
            "dedicated PostgreSQL is disabled; only spool custody and state.json remain active",
        );
    }

    let Some(address) = dedicated_postgres.listen_address.as_deref() else {
        return readiness_failed(
            "local-data-stores",
            true,
            "dedicated PostgreSQL is enabled but LPE_CT_LOCAL_DB_LISTEN_ADDRESS is missing",
        );
    };

    if address_binds_publicly(address) {
        return readiness_failed(
            "local-data-stores",
            true,
            format!("dedicated PostgreSQL bind {address} is public; port 5432 must stay private"),
        );
    }

    let has_database_url = env::var("LPE_CT_LOCAL_DB_URL")
        .ok()
        .is_some_and(|value| !value.trim().is_empty());
    if !has_database_url {
        return readiness_failed(
            "local-data-stores",
            true,
            "dedicated PostgreSQL is enabled but LPE_CT_LOCAL_DB_URL is missing",
        );
    }

    let purposes = dedicated_postgres.purposes.join(", ");
    readiness_ok(
        "local-data-stores",
        true,
        format!(
            "dedicated PostgreSQL is private on {address} for purposes: {purposes} ({})",
            dedicated_postgres.network_scope
        ),
    )
}

fn address_binds_publicly(address: &str) -> bool {
    let normalized = address.trim();
    if matches!(
        normalized,
        "0.0.0.0" | "0.0.0.0:5432" | "::" | "[::]" | "[::]:5432"
    ) {
        return true;
    }

    if let Ok(socket) = normalized.parse::<std::net::SocketAddr>() {
        return ip_is_public(socket.ip());
    }

    let host = if normalized.starts_with('[') {
        normalized
            .strip_prefix('[')
            .and_then(|value| value.split(']').next())
            .unwrap_or(normalized)
    } else {
        normalized
            .rsplit_once(':')
            .map(|(host, _)| host)
            .unwrap_or(normalized)
    };

    if matches!(host, "0.0.0.0" | "::" | "[::]") {
        return true;
    }

    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return ip_is_public(ip);
    }

    false
}

fn ip_is_public(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => !(ip.is_loopback() || ip.is_private()),
        std::net::IpAddr::V6(ip) => !(ip.is_loopback() || ip.is_unique_local()),
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

async fn check_optional_tcp_dependency(
    name: &str,
    target: &str,
    ok_detail: &str,
    warn_detail: &str,
) -> ReadinessCheck {
    let normalized = target.trim();
    if normalized.is_empty() {
        return readiness_warn(name, "no relay target configured for connectivity probing");
    }
    let address = smtp_target_socket_address(normalized);
    match tokio::time::timeout(
        Duration::from_millis(1_500),
        tokio::net::TcpStream::connect(&address),
    )
    .await
    {
        Ok(Ok(_)) => readiness_ok(name, false, ok_detail),
        Ok(Err(error)) => readiness_warn(
            name,
            format!("{warn_detail} ({normalized} -> {address}: {error})"),
        ),
        Err(_) => readiness_warn(
            name,
            format!("{warn_detail} ({normalized} -> {address}: timed out)"),
        ),
    }
}

fn check_spool_pressure(path: &Path) -> ReadinessCheck {
    let warn_threshold = env_u32("LPE_CT_READY_SPOOL_PRESSURE_WARN", 250);
    let deferred = count_queue_files(path, "deferred");
    let held = count_queue_files(path, "held");
    let outbound = count_queue_files(path, "outbound");
    let total = deferred + held + outbound;
    if total >= warn_threshold {
        readiness_warn(
            "spool-pressure",
            format!(
                "transport backlog is {total} message(s) across outbound={outbound}, deferred={deferred}, held={held}"
            ),
        )
    } else {
        readiness_ok(
            "spool-pressure",
            false,
            format!(
                "transport backlog is {total} message(s) across outbound={outbound}, deferred={deferred}, held={held}"
            ),
        )
    }
}

fn check_quarantine_backlog(path: &Path) -> ReadinessCheck {
    let warn_threshold = env_u32("LPE_CT_READY_QUARANTINE_BACKLOG_WARN", 50);
    let quarantined = count_queue_files(path, "quarantine");
    if quarantined >= warn_threshold {
        readiness_warn(
            "quarantine-backlog",
            format!("quarantine backlog is {quarantined} message(s)"),
        )
    } else {
        readiness_ok(
            "quarantine-backlog",
            false,
            format!("quarantine backlog is {quarantined} message(s)"),
        )
    }
}

fn count_queue_files(path: &Path, queue: &str) -> u32 {
    fs::read_dir(path.join(queue))
        .ok()
        .into_iter()
        .flat_map(|entries| entries.filter_map(std::result::Result::ok))
        .filter(|entry| entry.path().extension().and_then(|value| value.to_str()) == Some("json"))
        .count() as u32
}

fn env_u32(name: &str, default: u32) -> u32 {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .unwrap_or(default)
        .max(1)
}

fn smtp_target_socket_address(target: &str) -> String {
    let normalized = target
        .trim()
        .trim_start_matches("smtp://")
        .trim_start_matches("smtps://");
    if normalized.contains(':') {
        normalized.to_string()
    } else {
        format!("{normalized}:25")
    }
}

fn dkim_key_status(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return "not-configured".to_string();
    }
    let key_path = Path::new(trimmed);
    if !key_path.exists() {
        return "missing".to_string();
    }
    match fs::metadata(key_path) {
        Ok(metadata) if metadata.is_file() => "present".to_string(),
        Ok(_) => "invalid-path".to_string(),
        Err(_) => "unreadable".to_string(),
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, self.message).into_response()
    }
}

fn require_management_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<ManagementSession, ApiError> {
    let token = bearer_token(headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing bearer token"))?;
    let session = state
        .sessions
        .lock()
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "session lock poisoned"))?
        .get(&token)
        .cloned()
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "invalid management session"))?;
    Ok(session)
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn require_integration_request<T: serde::Serialize>(
    headers: &HeaderMap,
    path: &str,
    payload: &T,
) -> Result<(), ApiError> {
    let provided = headers
        .get(INTEGRATION_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            observability::record_security_event("integration_auth_failure");
            ApiError::new(StatusCode::UNAUTHORIZED, "missing integration key")
        })?;
    let expected = integration_shared_secret()
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if provided != expected {
        observability::record_security_event("integration_auth_failure");
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid integration key",
        ));
    }
    let signed = SignedIntegrationHeaders {
        integration_key: provided.to_string(),
        timestamp: required_header(headers, INTEGRATION_TIMESTAMP_HEADER)?,
        nonce: required_header(headers, INTEGRATION_NONCE_HEADER)?,
        signature: required_header(headers, INTEGRATION_SIGNATURE_HEADER)?,
    };
    signed
        .validate_payload(
            &expected,
            "POST",
            path,
            payload,
            current_unix_timestamp(),
            DEFAULT_MAX_SKEW_SECONDS,
        )
        .map_err(integration_auth_api_error)?;
    ensure_not_replayed(&signed).map_err(integration_auth_api_error)?;
    Ok(())
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| integration_auth_api_error(BridgeAuthError::MissingHeader(name)))
}

fn ensure_not_replayed(signed: &SignedIntegrationHeaders) -> Result<(), BridgeAuthError> {
    let cache = INTEGRATION_REPLAY_CACHE
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()));
    let now = current_unix_timestamp();
    let mut guard = cache.lock().map_err(|_| {
        BridgeAuthError::InvalidPayload("integration replay cache lock poisoned".to_string())
    })?;
    let cutoff = now - DEFAULT_MAX_SKEW_SECONDS;
    guard.retain(|_, seen_at| *seen_at >= cutoff);
    let key = signed.replay_key();
    if guard.insert(key, now).is_some() {
        return Err(BridgeAuthError::InvalidPayload(
            "integration request replay detected".to_string(),
        ));
    }
    Ok(())
}

fn integration_auth_api_error(error: BridgeAuthError) -> ApiError {
    observability::record_security_event("integration_auth_failure");
    ApiError::new(StatusCode::UNAUTHORIZED, error.to_string())
}

pub(crate) fn integration_shared_secret() -> Result<String> {
    let value = env::var("LPE_INTEGRATION_SHARED_SECRET")
        .map_err(|_| anyhow::anyhow!("LPE_INTEGRATION_SHARED_SECRET must be set"))?;
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        anyhow::bail!("LPE_INTEGRATION_SHARED_SECRET must not be empty");
    }
    if trimmed.len() < MIN_INTEGRATION_SECRET_LEN {
        anyhow::bail!(
            "LPE_INTEGRATION_SHARED_SECRET must contain at least {MIN_INTEGRATION_SECRET_LEN} characters"
        );
    }
    if is_known_weak_secret(&trimmed) {
        anyhow::bail!("LPE_INTEGRATION_SHARED_SECRET uses a forbidden weak placeholder value");
    }
    Ok(trimmed)
}

fn hash_password(password: &str) -> Result<String> {
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
        address_binds_publicly, apply_env_overrides, default_state, env_test_lock,
        ha_activation_check, ha_non_active_role_for_traffic, integration_shared_secret,
        normalize_local_data_stores, require_integration_request,
        submission_listener_is_configured, OUTBOUND_HANDOFF_PATH,
    };
    use axum::http::HeaderMap;
    use lpe_domain::{
        current_unix_timestamp, OutboundMessageHandoffRequest, SignedIntegrationHeaders,
        TransportRecipient, INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER,
        INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
    };
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };
    use uuid::Uuid;

    fn temp_file(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("lpe-ct-ha-role-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn integration_secret_must_be_present_and_strong() {
        let _guard = env_test_lock();
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
        assert!(integration_shared_secret().is_err());

        std::env::set_var("LPE_INTEGRATION_SHARED_SECRET", "change-me");
        assert!(integration_shared_secret().is_err());

        std::env::set_var(
            "LPE_INTEGRATION_SHARED_SECRET",
            "abcdef0123456789abcdef0123456789",
        );
        assert_eq!(
            integration_shared_secret().unwrap(),
            "abcdef0123456789abcdef0123456789"
        );
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn ha_role_check_accepts_only_active_role() {
        let _guard = env_test_lock();
        let role_file = temp_file("lpe-ct-ha-role");

        std::env::set_var("LPE_CT_HA_ROLE_FILE", &role_file);

        fs::write(&role_file, b"active\n").unwrap();
        let active = ha_activation_check();
        assert_eq!(active.status, "ok");

        fs::write(&role_file, b"drain\n").unwrap();
        let drain = ha_activation_check();
        assert_eq!(drain.status, "failed");
        assert!(drain.detail.contains("drain"));

        fs::write(&role_file, b"unknown\n").unwrap();
        let invalid = ha_activation_check();
        assert_eq!(invalid.status, "failed");
        assert!(invalid.detail.contains("unsupported role"));

        std::env::remove_var("LPE_CT_HA_ROLE_FILE");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn ha_non_active_gate_reports_non_active_roles() {
        let _guard = env_test_lock();
        let role_file = temp_file("lpe-ct-ha-gate");

        std::env::remove_var("LPE_CT_HA_ROLE_FILE");
        assert_eq!(ha_non_active_role_for_traffic().unwrap(), None);

        std::env::set_var("LPE_CT_HA_ROLE_FILE", &role_file);
        fs::write(&role_file, b"active\n").unwrap();
        assert_eq!(ha_non_active_role_for_traffic().unwrap(), None);

        fs::write(&role_file, b"standby\n").unwrap();
        assert_eq!(
            ha_non_active_role_for_traffic().unwrap().as_deref(),
            Some("standby")
        );

        std::env::remove_var("LPE_CT_HA_ROLE_FILE");
    }

    #[test]
    fn local_db_address_must_not_be_public() {
        assert!(!address_binds_publicly("127.0.0.1:5432"));
        assert!(!address_binds_publicly("10.20.0.15:5432"));
        assert!(!address_binds_publicly("[fd00::15]:5432"));
        assert!(address_binds_publicly("0.0.0.0:5432"));
        assert!(address_binds_publicly("[::]:5432"));
        assert!(address_binds_publicly("198.51.100.10:5432"));
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn env_overrides_enable_private_local_db_profile() {
        let _guard = env_test_lock();
        let mut state = default_state();

        std::env::set_var("LPE_CT_LOCAL_DB_ENABLED", "true");
        std::env::set_var("LPE_CT_LOCAL_DB_LISTEN_ADDRESS", "127.0.0.1:5432");
        std::env::set_var("LPE_CT_LOCAL_DB_NETWORK_SCOPE", "lpe-ct-cluster");
        std::env::set_var(
            "LPE_CT_LOCAL_DB_PURPOSES",
            "bayesian,reputation,quarantine-metadata",
        );

        apply_env_overrides(&mut state);
        normalize_local_data_stores(&mut state.local_data_stores);

        assert!(state.local_data_stores.dedicated_postgres.enabled);
        assert_eq!(
            state
                .local_data_stores
                .dedicated_postgres
                .listen_address
                .as_deref(),
            Some("127.0.0.1:5432")
        );
        assert_eq!(
            state.local_data_stores.dedicated_postgres.network_scope,
            "lpe-ct-cluster"
        );
        assert_eq!(
            state.local_data_stores.dedicated_postgres.purposes,
            vec![
                "bayesian".to_string(),
                "quarantine-metadata".to_string(),
                "reputation".to_string()
            ]
        );

        std::env::remove_var("LPE_CT_LOCAL_DB_ENABLED");
        std::env::remove_var("LPE_CT_LOCAL_DB_LISTEN_ADDRESS");
        std::env::remove_var("LPE_CT_LOCAL_DB_NETWORK_SCOPE");
        std::env::remove_var("LPE_CT_LOCAL_DB_PURPOSES");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn submission_listener_requires_bind_and_tls_material() {
        let _guard = env_test_lock();
        std::env::remove_var("LPE_CT_SUBMISSION_TLS_CERT_PATH");
        std::env::remove_var("LPE_CT_SUBMISSION_TLS_KEY_PATH");
        assert!(!submission_listener_is_configured(&Some(
            "0.0.0.0:465".to_string()
        )));

        std::env::set_var(
            "LPE_CT_SUBMISSION_TLS_CERT_PATH",
            "/etc/lpe-ct/fullchain.pem",
        );
        std::env::set_var("LPE_CT_SUBMISSION_TLS_KEY_PATH", "/etc/lpe-ct/privkey.pem");
        assert!(submission_listener_is_configured(&Some(
            "0.0.0.0:465".to_string()
        )));
        assert!(!submission_listener_is_configured(&None));

        std::env::remove_var("LPE_CT_SUBMISSION_TLS_CERT_PATH");
        std::env::remove_var("LPE_CT_SUBMISSION_TLS_KEY_PATH");
    }

    #[test]
    #[ignore = "env-sensitive"]
    fn signed_integration_requests_reject_replay() {
        let _guard = env_test_lock();
        std::env::set_var(
            "LPE_INTEGRATION_SHARED_SECRET",
            "abcdef0123456789abcdef0123456789",
        );
        let payload = OutboundMessageHandoffRequest {
            queue_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            from_address: "sender@example.test".to_string(),
            from_display: None,
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            to: vec![TransportRecipient {
                address: "dest@example.test".to_string(),
                display_name: None,
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Signed".to_string(),
            body_text: "Body".to_string(),
            body_html_sanitized: None,
            internet_message_id: None,
            attempt_count: 0,
            last_attempt_error: None,
        };
        let signed = SignedIntegrationHeaders::sign_with_timestamp_and_nonce(
            "abcdef0123456789abcdef0123456789",
            "POST",
            OUTBOUND_HANDOFF_PATH,
            &payload,
            current_unix_timestamp(),
            "nonce-outbound-1",
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            INTEGRATION_KEY_HEADER,
            signed.integration_key.parse().unwrap(),
        );
        headers.insert(
            INTEGRATION_TIMESTAMP_HEADER,
            signed.timestamp.parse().unwrap(),
        );
        headers.insert(INTEGRATION_NONCE_HEADER, signed.nonce.parse().unwrap());
        headers.insert(
            INTEGRATION_SIGNATURE_HEADER,
            signed.signature.parse().unwrap(),
        );

        require_integration_request(&headers, OUTBOUND_HANDOFF_PATH, &payload).unwrap();
        assert!(require_integration_request(&headers, OUTBOUND_HANDOFF_PATH, &payload).is_err());
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
    }
}
