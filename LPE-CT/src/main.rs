use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, put},
    Json, Router,
};
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
    mutual_tls_required: bool,
    fallback_to_hold_queue: bool,
    sync_interval_seconds: u32,
    lan_dependency_note: String,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let bind_address =
        env::var("LPE_CT_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8380".to_string());
    let state_file = PathBuf::from(
        env::var("LPE_CT_STATE_FILE").unwrap_or_else(|_| "/var/lib/lpe-ct/state.json".to_string()),
    );

    let state = AppState {
        store: Arc::new(Mutex::new(load_or_initialize_state(&state_file)?)),
        state_file: Arc::new(state_file),
    };

    let listener = TcpListener::bind(&bind_address).await?;
    info!("lpe-ct management api listening on http://{bind_address}");
    axum::serve(listener, router(state)).await?;
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
    Ok(Json(read_state(&state)?))
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
            mutual_tls_required: true,
            fallback_to_hold_queue: true,
            sync_interval_seconds: 30,
            lan_dependency_note: "Only relay and management flows to the LAN are allowed.".to_string(),
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
            require_dkim_alignment: true,
            require_dmarc_enforcement: true,
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
            inbound_messages: 1428,
            deferred_messages: 12,
            quarantined_messages: 4,
            held_messages: 0,
            delivery_attempts_last_hour: 381,
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
