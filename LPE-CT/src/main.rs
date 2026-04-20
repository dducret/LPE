use anyhow::{Context, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    middleware,
    routing::{get, post, put},
    Json, Router,
};
use lpe_domain::{OutboundMessageHandoffRequest, OutboundMessageHandoffResponse};
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tracing::info;
use uuid::Uuid;

mod observability;
mod smtp;

const MIN_INTEGRATION_SECRET_LEN: usize = 32;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolicySettings {
    drain_mode: bool,
    quarantine_enabled: bool,
    greylisting_enabled: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    policies: PolicySettings,
    updates: UpdateSettings,
    queues: QueueMetrics,
    management_auth: ManagementAuthState,
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
    let state_file = PathBuf::from(
        env::var("LPE_CT_STATE_FILE").unwrap_or_else(|_| "/var/lib/lpe-ct/state.json".to_string()),
    );
    let spool_dir = PathBuf::from(
        env::var("LPE_CT_SPOOL_DIR").unwrap_or_else(|_| "/var/spool/lpe-ct".to_string()),
    );
    integration_shared_secret()?;
    smtp::initialize_spool(&spool_dir)?;

    let mut dashboard = load_or_initialize_state(&state_file)?;
    apply_env_overrides(&mut dashboard);
    ensure_management_bootstrap(&mut dashboard)?;
    normalize_policy_settings(&mut dashboard.policies);
    dashboard.site.management_bind = bind_address.clone();
    dashboard.site.public_smtp_bind = smtp_bind_address.clone();
    persist_state(&state_file, &dashboard)?;

    let state = AppState {
        store: Arc::new(Mutex::new(dashboard)),
        sessions: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        state_file: Arc::new(state_file),
        spool_dir: Arc::new(spool_dir),
    };

    let api_state = state.clone();
    let smtp_state_file = state.state_file.as_ref().clone();
    let smtp_spool_dir = state.spool_dir.as_ref().clone();
    let api_task = tokio::spawn(async move {
        let listener = TcpListener::bind(&bind_address).await?;
        info!("lpe-ct management api listening on http://{bind_address}");
        axum::serve(listener, router(api_state)).await?;
        Result::<()>::Ok(())
    });
    let smtp_task = tokio::spawn(async move {
        smtp::run_smtp_listener(smtp_bind_address, smtp_state_file, smtp_spool_dir).await
    });

    tokio::select! {
        result = api_task => result??,
        result = smtp_task => result??,
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
    mutate_state(&state, &admin.email, "update-policies", move |dashboard| {
        dashboard.policies = payload;
    })
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

async fn outbound_handoff(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<OutboundMessageHandoffRequest>,
) -> Result<Json<OutboundMessageHandoffResponse>, ApiError> {
    let request_trace_id = observability::trace_id_from_headers(&headers);
    let queue_id = payload.queue_id;
    let message_id = payload.message_id;
    let internet_message_id = payload.internet_message_id.clone();
    require_integration_key(&headers)?;
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

fn read_state(state: &AppState) -> Result<DashboardState, ApiError> {
    state
        .store
        .lock()
        .map(|guard| guard.clone())
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))
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
}

fn normalize_policy_settings(policies: &mut PolicySettings) {
    if policies.reputation_reject_threshold > policies.reputation_quarantine_threshold {
        policies.reputation_reject_threshold = policies.reputation_quarantine_threshold;
    }
    if policies.spam_reject_threshold < policies.spam_quarantine_threshold {
        policies.spam_reject_threshold = policies.spam_quarantine_threshold;
    }
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
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
        policies: PolicySettings {
            drain_mode: false,
            quarantine_enabled: true,
            greylisting_enabled: true,
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
        },
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

fn default_dnsbl_enabled() -> bool {
    true
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
    let required = ["incoming", "deferred", "quarantine", "held", "sent"];
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

fn require_integration_key(headers: &HeaderMap) -> Result<(), ApiError> {
    let provided = headers
        .get("x-lpe-integration-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            observability::record_security_event("integration_auth_failure");
            ApiError::new(StatusCode::UNAUTHORIZED, "missing integration key")
        })?;
    let expected = integration_shared_secret()
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if provided == expected {
        Ok(())
    } else {
        observability::record_security_event("integration_auth_failure");
        Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid integration key",
        ))
    }
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
    use super::{ha_activation_check, integration_shared_secret};
    use std::{
        fs,
        path::PathBuf,
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    };

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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
    fn integration_secret_must_be_present_and_strong() {
        let _guard = ENV_LOCK.lock().unwrap();
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
    fn ha_role_check_accepts_only_active_role() {
        let _guard = ENV_LOCK.lock().unwrap();
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
}
