use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post, put},
    Json, Router,
};
use lpe_domain::{OutboundMessageHandoffRequest, OutboundMessageHandoffResponse};
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

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
    #[serde(default = "default_dnsbl_enabled")]
    dnsbl_enabled: bool,
    #[serde(default = "default_dnsbl_zones")]
    dnsbl_zones: Vec<String>,
    #[serde(default = "default_reputation_enabled")]
    reputation_enabled: bool,
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
    audit: Vec<AuditEvent>,
}

#[derive(Debug, Clone, Serialize)]
struct HealthResponse {
    status: String,
    service: String,
    node_name: String,
    role: String,
}

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<DashboardState>>,
    state_file: Arc<PathBuf>,
    spool_dir: Arc<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

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
    dashboard.site.management_bind = bind_address.clone();
    dashboard.site.public_smtp_bind = smtp_bind_address.clone();
    persist_state(&state_file, &dashboard)?;

    let state = AppState {
        store: Arc::new(Mutex::new(dashboard)),
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

async fn dashboard(State(state): State<AppState>) -> Result<Json<DashboardState>, ApiError> {
    let mut snapshot = read_state(&state)?;
    snapshot.queues = smtp::queue_metrics(&state.spool_dir, snapshot.queues.upstream_reachable)
        .map_err(ApiError::from)?;
    Ok(Json(snapshot))
}

async fn update_site(
    State(state): State<AppState>,
    Json(payload): Json<SiteProfile>,
) -> Result<Json<DashboardState>, ApiError> {
    mutate_state(&state, "update-site", move |dashboard| {
        dashboard.site = payload;
    })
}

async fn update_relay(
    State(state): State<AppState>,
    Json(payload): Json<RelaySettings>,
) -> Result<Json<DashboardState>, ApiError> {
    mutate_state(&state, "update-relay", move |dashboard| {
        dashboard.relay = payload;
    })
}

async fn update_network(
    State(state): State<AppState>,
    Json(payload): Json<NetworkSettings>,
) -> Result<Json<DashboardState>, ApiError> {
    mutate_state(&state, "update-network", move |dashboard| {
        dashboard.network = payload;
    })
}

async fn update_policies(
    State(state): State<AppState>,
    Json(payload): Json<PolicySettings>,
) -> Result<Json<DashboardState>, ApiError> {
    mutate_state(&state, "update-policies", move |dashboard| {
        dashboard.policies = payload;
    })
}

async fn update_updates(
    State(state): State<AppState>,
    Json(payload): Json<UpdateSettings>,
) -> Result<Json<DashboardState>, ApiError> {
    mutate_state(&state, "update-updates", move |dashboard| {
        dashboard.updates = payload;
    })
}

async fn outbound_handoff(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<OutboundMessageHandoffRequest>,
) -> Result<Json<OutboundMessageHandoffResponse>, ApiError> {
    require_integration_key(&headers)?;
    let snapshot = read_state(&state)?;
    let runtime = smtp::runtime_config_from_dashboard(&snapshot);
    let response = smtp::process_outbound_handoff(&state.spool_dir, &runtime, payload)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

fn mutate_state<F>(
    state: &AppState,
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
    append_audit_event(&mut guard, action);
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

fn append_audit_event(state: &mut DashboardState, action: &str) {
    state.audit.insert(
        0,
        AuditEvent {
            timestamp: current_timestamp(),
            actor: "management-ui".to_string(),
            action: action.to_string(),
            details: "DMZ sorting center configuration updated".to_string(),
        },
    );
    state.audit.truncate(12);
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
    if let Ok(value) = env::var("LPE_CT_DNSBL_ENABLED") {
        state.policies.dnsbl_enabled = parse_bool(&value);
    }
    if let Ok(value) = env::var("LPE_CT_DNSBL_ZONES") {
        state.policies.dnsbl_zones = parse_csv(&value);
    }
    if let Ok(value) = env::var("LPE_CT_REPUTATION_ENABLED") {
        state.policies.reputation_enabled = parse_bool(&value);
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

fn parse_bool(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
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
    DashboardState {
        site: SiteProfile {
            node_name: "ct-edge-01".to_string(),
            role: "dmz-sorting-center".to_string(),
            region: "eu-central".to_string(),
            dmz_zone: "mail-dmz-a".to_string(),
            published_mx: "mx1.example.test".to_string(),
            management_fqdn: "ct-mgmt.example.test".to_string(),
            public_smtp_bind: "0.0.0.0:25".to_string(),
            management_bind: "127.0.0.1:8380".to_string(),
        },
        relay: RelaySettings {
            primary_upstream: "smtp://10.20.0.12:2525".to_string(),
            secondary_upstream: "smtp://10.20.0.13:2525".to_string(),
            core_delivery_base_url: default_core_delivery_base_url(),
            mutual_tls_required: false,
            fallback_to_hold_queue: false,
            sync_interval_seconds: 30,
            lan_dependency_note: "Only relay and management flows to the LAN are allowed."
                .to_string(),
        },
        routing: RoutingSettings {
            rules: vec![RoutingRule {
                id: "default-primary".to_string(),
                description: "Default outbound route through the primary relay".to_string(),
                sender_domain: None,
                recipient_domain: None,
                relay_target: "smtp://10.20.0.12:2525".to_string(),
            }],
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
            allowed_management_cidrs: vec!["10.20.30.0/24".to_string()],
            allowed_upstream_cidrs: vec!["10.20.0.0/16".to_string()],
            outbound_smart_hosts: vec![
                "10.20.0.12:2525".to_string(),
                "10.20.0.13:2525".to_string(),
            ],
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
            dnsbl_enabled: default_dnsbl_enabled(),
            dnsbl_zones: default_dnsbl_zones(),
            reputation_enabled: default_reputation_enabled(),
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
            update_source: "https://github.com/dducret/LPE".to_string(),
        },
        queues: QueueMetrics {
            inbound_messages: 0,
            deferred_messages: 0,
            quarantined_messages: 0,
            held_messages: 0,
            delivery_attempts_last_hour: 0,
            upstream_reachable: true,
        },
        audit: vec![
            AuditEvent {
                timestamp: "bootstrap".to_string(),
                actor: "system".to_string(),
                action: "seed-profile".to_string(),
                details: "Default DMZ sorting center profile created".to_string(),
            },
            AuditEvent {
                timestamp: "bootstrap".to_string(),
                actor: "system".to_string(),
                action: "seed-relay".to_string(),
                details: "Initial LAN relay targets prepared".to_string(),
            },
        ],
    }
}

fn default_core_delivery_base_url() -> String {
    "http://10.20.0.20:8080".to_string()
}

fn default_dnsbl_enabled() -> bool {
    true
}

fn default_dnsbl_zones() -> Vec<String> {
    vec!["zen.spamhaus.org".to_string(), "bl.spamcop.net".to_string()]
}

fn default_reputation_enabled() -> bool {
    true
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

fn require_integration_key(headers: &HeaderMap) -> Result<(), ApiError> {
    let provided = headers
        .get("x-lpe-integration-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing integration key"))?;
    let expected = integration_shared_secret()
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if provided == expected {
        Ok(())
    } else {
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
    use super::integration_shared_secret;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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
}
