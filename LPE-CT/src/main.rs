use anyhow::{Context, Result};
use axum::{
    extract::{Path as AxumPath, Query, State},
    http::{
        header::{CONTENT_DISPOSITION, CONTENT_TYPE},
        HeaderMap, HeaderValue, StatusCode,
    },
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use lpe_domain::{
    current_unix_timestamp, BridgeAuthError, OutboundMessageHandoffRequest,
    OutboundMessageHandoffResponse, RecipientVerificationRequest, RecipientVerificationResponse,
    SignedIntegrationHeaders, DEFAULT_MAX_SKEW_SECONDS, INTEGRATION_KEY_HEADER,
    INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
};
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tokio_rustls::rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    ServerConfig,
};
use tracing::{info, warn};
use uuid::Uuid;

mod dashboard_config;
mod dkim_signing;
mod host_logs;
mod http_routes;
mod imaps_proxy;
mod management_auth;
mod observability;
mod outlook_test_message;
mod readiness;
mod reporting;
mod smtp;
mod storage;
mod submission;
mod system_actions;
mod system_diagnostics;
mod system_metrics;
mod transport_policy;
pub(crate) use dashboard_config::{
    accepted_domain_from_input, apply_env_overrides, default_antivirus_enabled,
    default_antivirus_fail_closed, default_antivirus_provider_chain, default_bayespam_auto_learn,
    default_bayespam_enabled, default_bayespam_max_tokens, default_bayespam_min_token_length,
    default_bayespam_score_weight, default_core_delivery_base_url, default_defer_on_auth_tempfail,
    default_dkim_headers, default_dkim_settings, default_dnsbl_enabled, default_dnsbl_zones,
    default_forbidden_canonical_data, default_local_db_network_scope, default_local_db_notes,
    default_local_db_purposes, default_policy_artifacts,
    default_recipient_verification_cache_ttl_seconds, default_recipient_verification_settings,
    default_reputation_enabled, default_reputation_quarantine_threshold,
    default_reputation_reject_threshold, default_spam_quarantine_threshold,
    default_spam_reject_threshold, default_spool_queues, default_state, default_true,
    ensure_management_bootstrap, normalize_accepted_domains, normalize_local_data_stores,
    normalize_policy_settings,
    normalize_public_tls_settings, normalize_relay_settings, probe_lpe_core_delivery,
    probe_lpe_recipient_bridge, submission_listener_is_configured, validate_relay_settings,
};
#[cfg(test)]
pub(crate) use dashboard_config::{lpe_bridge_probe_url, lpe_health_probe_url};
pub(crate) use http_routes::{
    accepted_domains, connect_lpe_support, create_accepted_domain, dashboard,
    delete_accepted_domain, delete_host_log, delete_public_tls_profile, delete_trace,
    digest_report_details, digest_reports, download_host_log, flush_mail_queue, health,
    health_live, health_ready, host_log_content, host_logs_list, import_accepted_domains, login,
    logout, mail_history, me, outbound_handoff, policy_status, quarantine_items, release_trace,
    reporting_snapshot, retry_trace, route_diagnostics, run_apt_update_upgrade,
    run_digest_reports, run_spam_test, run_system_power_action, run_system_tool,
    select_public_tls_profile, sync_system_ntp, system_diagnostic_report,
    system_diagnostic_service_action, system_diagnostic_services, system_health_check,
    test_accepted_domain, trace_details, trace_history, update_accepted_domain,
    update_network, update_policies, update_relay, update_reporting, update_site,
    update_system_ntp, update_updates, upload_public_tls_profile,
};
#[cfg(test)]
pub(crate) use http_routes::mark_accepted_domain_verified;
use management_auth::{
    bearer_token, hash_password, is_known_weak_secret, require_management_admin, verify_password,
    ApiError,
};
pub(crate) use management_auth::{integration_shared_secret, require_integration_request};
pub(crate) use readiness::ha_non_active_role_for_traffic;
#[cfg(test)]
pub(crate) use readiness::address_binds_publicly;
use readiness::{
    check_dashboard_state_store, check_local_data_store_policy, check_non_empty_value,
    check_optional_http_dependency, check_optional_tcp_dependency, check_quarantine_backlog,
    check_spool_layout, check_spool_pressure, dkim_key_status, ha_activation_check,
    readiness_failed, readiness_ok, readiness_status,
};

const OUTBOUND_HANDOFF_PATH: &str = "/api/v1/integration/outbound-messages";
const ENV_PUBLIC_TLS_PROFILE_ID: &str = "env-public";

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
    primary_upstream: String,
    secondary_upstream: String,
    #[serde(default)]
    outbound_ehlo_name: String,
    #[serde(default = "default_core_delivery_base_url")]
    core_delivery_base_url: String,
    mutual_tls_required: bool,
    fallback_to_hold_queue: bool,
    sync_interval_seconds: u32,
    lan_dependency_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcceptedDomain {
    id: String,
    domain: String,
    destination_server: String,
    verification_type: String,
    rbl_checks: bool,
    spf_checks: bool,
    greylisting: bool,
    #[serde(default = "default_true")]
    accept_null_reverse_path: bool,
    verified: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct AcceptedDomainInput {
    domain: String,
    destination_server: String,
    verification_type: String,
    rbl_checks: bool,
    spf_checks: bool,
    greylisting: bool,
    #[serde(default = "default_true")]
    accept_null_reverse_path: bool,
    verified: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ImportAcceptedDomainsRequest {
    domains: Vec<AcceptedDomainInput>,
}

#[derive(Debug, Clone, Serialize)]
struct AcceptedDomainTestResponse {
    domain: String,
    destination_server: String,
    verified: bool,
    checked_url: String,
    checked_bridge_url: String,
    bridge_reachable: bool,
    recipient_verified: bool,
    detail: String,
}

#[derive(Debug, Deserialize)]
struct LpeHealthProbeResponse {
    service: Option<String>,
    status: Option<String>,
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
    #[serde(default)]
    public_tls: PublicTlsSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PublicTlsSettings {
    #[serde(default)]
    active_profile_id: Option<String>,
    #[serde(default)]
    profiles: Vec<PublicTlsProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublicTlsProfile {
    id: String,
    name: String,
    cert_path: String,
    key_path: String,
    created_at: String,
}

#[derive(Debug, Clone, Deserialize)]
struct PublicTlsUploadRequest {
    name: String,
    certificate_pem: String,
    private_key_pem: String,
    #[serde(default)]
    activate: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct PublicTlsSelectionRequest {
    profile_id: Option<String>,
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
    #[serde(default)]
    incoming_messages: u32,
    #[serde(default)]
    active_messages: u32,
    deferred_messages: u32,
    quarantined_messages: u32,
    held_messages: u32,
    #[serde(default)]
    corrupt_messages: u32,
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
    accepted_domains: Vec<AcceptedDomain>,
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
struct DashboardResponse {
    #[serde(flatten)]
    state: DashboardState,
    system: system_metrics::SystemMetrics,
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
    install_rustls_crypto_provider();

    let bind_address =
        env::var("LPE_CT_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8380".to_string());
    let smtp_bind_address =
        env::var("LPE_CT_SMTP_BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0:25".to_string());
    let submission_bind_address = env::var("LPE_CT_SUBMISSION_BIND_ADDRESS").ok();
    let imaps_bind_address = imaps_proxy::imaps_bind_address();
    let imaps_upstream_address = imaps_proxy::imaps_upstream_address();
    let imaps_tls_cert_path = imaps_proxy::imaps_tls_cert_path();
    let imaps_tls_key_path = imaps_proxy::imaps_tls_key_path();
    let state_file = PathBuf::from(
        env::var("LPE_CT_STATE_FILE").unwrap_or_else(|_| "/var/lib/lpe-ct/state.json".to_string()),
    );
    let spool_dir = PathBuf::from(
        env::var("LPE_CT_SPOOL_DIR").unwrap_or_else(|_| "/var/spool/lpe-ct".to_string()),
    );
    smtp::initialize_spool(&spool_dir)?;

    let mut bootstrap_dashboard = load_or_initialize_state(&state_file)?;
    apply_env_overrides(&mut bootstrap_dashboard);
    normalize_local_data_stores(&mut bootstrap_dashboard.local_data_stores);
    let local_db_config = local_db_config_from_dashboard(&bootstrap_dashboard);
    let mut dashboard = storage::load_dashboard_state(&local_db_config)
        .await?
        .unwrap_or(bootstrap_dashboard);
    apply_env_overrides(&mut dashboard);
    ensure_management_bootstrap(&mut dashboard)?;
    normalize_accepted_domains(&mut dashboard.accepted_domains);
    normalize_policy_settings(&mut dashboard.policies);
    normalize_public_tls_settings(&mut dashboard.network.public_tls);
    normalize_local_data_stores(&mut dashboard.local_data_stores);
    reporting::normalize_reporting_settings(&mut dashboard.reporting);
    let site_profile = dashboard.site.clone();
    normalize_relay_settings(&mut dashboard.relay, &site_profile);
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
    sync_dashboard_to_postgres(&dashboard).await?;

    let state = AppState {
        store: Arc::new(Mutex::new(dashboard)),
        sessions: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        state_file: Arc::new(state_file),
        spool_dir: Arc::new(spool_dir),
    };

    let api_state = state.clone();
    let smtp_dashboard_store = state.store.clone();
    let smtp_spool_dir = state.spool_dir.as_ref().clone();
    let submission_dashboard_store = state.store.clone();
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
        smtp::run_smtp_listener(smtp_bind_address, smtp_dashboard_store, smtp_spool_dir).await
    });
    let submission_task = tokio::spawn(async move {
        if let Some(bind_address) = submission_bind_address {
            submission::run_submission_listener(
                bind_address,
                submission_core_base_url,
                submission_dashboard_store,
            )
            .await?;
        } else {
            let _ = std::future::pending::<Result<()>>().await;
        }
        Result::<()>::Ok(())
    });
    let imaps_task = tokio::spawn(async move {
        if let Some(bind_address) = imaps_bind_address {
            let cert_path = imaps_tls_cert_path
                .ok_or_else(|| anyhow::anyhow!("LPE_CT_IMAPS_TLS_CERT_PATH or LPE_CT_PUBLIC_TLS_CERT_PATH must be set when LPE_CT_IMAPS_BIND_ADDRESS is configured"))?;
            let key_path = imaps_tls_key_path
                .ok_or_else(|| anyhow::anyhow!("LPE_CT_IMAPS_TLS_KEY_PATH or LPE_CT_PUBLIC_TLS_KEY_PATH must be set when LPE_CT_IMAPS_BIND_ADDRESS is configured"))?;
            imaps_proxy::run_imaps_proxy(bind_address, imaps_upstream_address, cert_path, key_path)
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
        result = imaps_task => result??,
    }

    Ok(())
}

fn install_rustls_crypto_provider() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
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
        .route("/api/v1/host-logs/{category}", get(host_logs_list))
        .route(
            "/api/v1/host-logs/{category}/{log_id}",
            get(host_log_content).delete(delete_host_log),
        )
        .route(
            "/api/v1/host-logs/{category}/{log_id}/download",
            get(download_host_log),
        )
        .route("/api/v1/routes/diagnostics", get(route_diagnostics))
        .route(
            "/api/v1/accepted-domains",
            get(accepted_domains).post(create_accepted_domain),
        )
        .route(
            "/api/v1/accepted-domains/import",
            post(import_accepted_domains),
        )
        .route(
            "/api/v1/accepted-domains/{domain_id}",
            put(update_accepted_domain).delete(delete_accepted_domain),
        )
        .route(
            "/api/v1/accepted-domains/{domain_id}/test",
            post(test_accepted_domain),
        )
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
        .route(
            "/api/v1/system-diagnostics/services",
            get(system_diagnostic_services),
        )
        .route(
            "/api/v1/system-diagnostics/services/{service_id}/{action}",
            post(system_diagnostic_service_action),
        )
        .route(
            "/api/v1/system-diagnostics/{kind}",
            get(system_diagnostic_report),
        )
        .route(
            "/api/v1/system-diagnostics/health-check",
            post(system_health_check),
        )
        .route("/api/v1/system-diagnostics/tools", post(run_system_tool))
        .route("/api/v1/system-diagnostics/spam-test", post(run_spam_test))
        .route(
            "/api/v1/system-diagnostics/support-connect",
            post(connect_lpe_support),
        )
        .route(
            "/api/v1/system-diagnostics/flush-mail-queue",
            post(flush_mail_queue),
        )
        .route(
            "/api/v1/public-tls/profiles",
            post(upload_public_tls_profile),
        )
        .route("/api/v1/public-tls/select", put(select_public_tls_profile))
        .route(
            "/api/v1/public-tls/profiles/{profile_id}",
            axum::routing::delete(delete_public_tls_profile),
        )
        .route("/api/v1/site", put(update_site))
        .route("/api/v1/relay", put(update_relay))
        .route("/api/v1/network", put(update_network))
        .route("/api/v1/system-time/ntp", put(update_system_ntp))
        .route("/api/v1/system-time/sync", post(sync_system_ntp))
        .route(
            "/api/v1/system-updates/apt-upgrade",
            post(run_apt_update_upgrade),
        )
        .route(
            "/api/v1/system-power/{action}",
            post(run_system_power_action),
        )
        .route("/api/v1/policies", put(update_policies))
        .route("/api/v1/updates", put(update_updates))
        .route(
            "/api/v1/integration/outbound-messages",
            post(outbound_handoff),
        )
        .layer(middleware::from_fn(observability::observe_http))
        .with_state(state)
}

async fn mutate_state<F>(
    state: &AppState,
    actor: &str,
    action: &str,
    update: F,
) -> Result<Json<DashboardState>, ApiError>
where
    F: FnOnce(&mut DashboardState),
{
    let snapshot = {
        let mut guard = state
            .store
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "state lock poisoned"))?;
        update(&mut guard);
        append_dashboard_audit_event(&mut guard, actor, action);
        persist_state(&state.state_file, &guard)?;
        guard.clone()
    };
    sync_dashboard_to_postgres(&snapshot)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(snapshot))
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
        let (generated_reports, snapshot) = {
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
                (0, None)
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
                (generated_reports.len(), Some(guard.clone()))
            }
        };
        if let Some(snapshot) = snapshot {
            if let Err(error) = sync_dashboard_to_postgres(&snapshot).await {
                tracing::warn!(
                    error = %error,
                    "unable to persist scheduled reporting state to PostgreSQL"
                );
            }
        }
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
    sync_dashboard_to_postgres(&snapshot)
        .await
        .map_err(ApiError::from)
}

async fn sync_dashboard_to_postgres(snapshot: &DashboardState) -> Result<()> {
    let config = local_db_config_from_dashboard(snapshot);
    storage::persist_dashboard_state(&config, snapshot).await?;
    storage::sync_dashboard_configuration(&config, snapshot).await?;
    Ok(())
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

async fn append_audit_event_with_actor(
    state: &AppState,
    actor: &str,
    action: &str,
    details: &str,
) -> Result<(), ApiError> {
    let snapshot = {
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
        guard.clone()
    };
    sync_dashboard_to_postgres(&snapshot)
        .await
        .map_err(ApiError::from)?;
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

fn public_tls_store_dir(state: &AppState) -> PathBuf {
    env::var("LPE_CT_PUBLIC_TLS_STORE_DIR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            state
                .state_file
                .as_ref()
                .parent()
                .map(|parent| parent.join("public-tls"))
                .unwrap_or_else(|| PathBuf::from("/var/lib/lpe-ct/public-tls"))
        })
}

fn store_public_tls_profile(
    state: &AppState,
    payload: PublicTlsUploadRequest,
) -> Result<(PublicTlsProfile, bool)> {
    let name = payload.name.trim();
    if name.is_empty() {
        anyhow::bail!("public TLS profile name is required");
    }
    validate_tls_pair_from_pem(&payload.certificate_pem, &payload.private_key_pem)?;

    let profile_id = Uuid::new_v4().to_string();
    let store_dir = public_tls_store_dir(state);
    fs::create_dir_all(&store_dir)
        .with_context(|| format!("unable to create public TLS store {}", store_dir.display()))?;

    let cert_path = store_dir.join(format!("{profile_id}.cert.pem"));
    let key_path = store_dir.join(format!("{profile_id}.key.pem"));
    fs::write(&cert_path, normalize_pem_text(&payload.certificate_pem)).with_context(|| {
        format!(
            "unable to write public TLS certificate {}",
            cert_path.display()
        )
    })?;
    write_private_key_file(&key_path, &normalize_pem_text(&payload.private_key_pem)).with_context(
        || {
            format!(
                "unable to write public TLS private key {}",
                key_path.display()
            )
        },
    )?;

    Ok((
        PublicTlsProfile {
            id: profile_id,
            name: name.to_string(),
            cert_path: cert_path.display().to_string(),
            key_path: key_path.display().to_string(),
            created_at: current_timestamp(),
        },
        payload.activate,
    ))
}

fn normalize_pem_text(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    format!("{trimmed}\n")
}

fn write_private_key_file(path: &Path, value: &str) -> Result<()> {
    fs::write(path, value)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn validate_tls_pair_from_paths(cert_path: &str, key_path: &str) -> Result<()> {
    let cert_pem = fs::read_to_string(cert_path)
        .with_context(|| format!("unable to read certificate {cert_path}"))?;
    let key_pem = fs::read_to_string(key_path)
        .with_context(|| format!("unable to read private key {key_path}"))?;
    validate_tls_pair_from_pem(&cert_pem, &key_pem)
}

fn validate_tls_pair_from_pem(cert_pem: &str, key_pem: &str) -> Result<()> {
    let certificates = parse_certificates_pem(cert_pem)?;
    let key = parse_private_key_pem(key_pem)?;
    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .context("public TLS certificate and private key do not form a usable server identity")?;
    Ok(())
}

fn parse_certificates_pem(value: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = std::io::BufReader::new(value.as_bytes());
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("unable to parse public TLS certificate PEM")?;
    if certificates.is_empty() {
        anyhow::bail!("public TLS certificate PEM does not contain a certificate");
    }
    Ok(certificates)
}

fn parse_private_key_pem(value: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader = std::io::BufReader::new(value.as_bytes());
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("unable to parse PKCS#8 private key PEM")?;
    if let Some(key) = keys.pop() {
        return Ok(PrivateKeyDer::Pkcs8(key));
    }

    let mut reader = std::io::BufReader::new(value.as_bytes());
    let mut keys = rustls_pemfile::rsa_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("unable to parse RSA private key PEM")?;
    let Some(key) = keys.pop() else {
        anyhow::bail!("private key PEM does not contain a supported private key");
    };
    Ok(PrivateKeyDer::Pkcs1(key))
}

fn current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format!("unix:{}", duration.as_secs()),
        Err(_) => "unix:0".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        address_binds_publicly, apply_env_overrides, default_state, env_test_lock,
        ha_activation_check, ha_non_active_role_for_traffic, integration_shared_secret,
        lpe_bridge_probe_url, lpe_health_probe_url, mark_accepted_domain_verified,
        normalize_local_data_stores, persist_state, require_integration_request,
        submission_listener_is_configured, AcceptedDomain, DashboardResponse,
        OUTBOUND_HANDOFF_PATH,
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

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("lpe-ct-{name}-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn dashboard_response_serializes_runtime_system_without_persisting_it() {
        let state = default_state();
        let spool = temp_dir("dashboard-system");
        let response = DashboardResponse {
            state: state.clone(),
            system: crate::system_metrics::collect(&spool),
        };

        let response_json = serde_json::to_value(&response).unwrap();
        assert!(response_json.get("system").is_some());
        assert!(response_json["system"].get("host_time").is_some());
        assert!(response_json["system"].get("hostname").is_some());
        assert!(response_json["system"].get("architecture").is_some());

        let persisted_state_json = serde_json::to_value(&state).unwrap();
        assert!(persisted_state_json.get("system").is_none());

        let state_file = spool.join("state.json");
        persist_state(&state_file, &state).unwrap();
        let raw = fs::read_to_string(state_file).unwrap();
        assert!(!raw.contains("\"system\""));
    }

    #[test]
    fn queue_metrics_count_runtime_spool_messages_by_state() {
        let spool = temp_dir("queue-metrics");
        crate::smtp::initialize_spool(&spool).unwrap();
        fs::write(spool.join("incoming").join("incoming-1.json"), "{}").unwrap();
        fs::write(spool.join("outbound").join("outbound-1.json"), "{}").unwrap();
        fs::write(spool.join("outbound").join("outbound-2.json"), "{").unwrap();
        fs::write(spool.join("outbound").join("outbound.tmp"), "{").unwrap();
        fs::write(spool.join("deferred").join("deferred-1.json"), "{}").unwrap();
        fs::write(spool.join("held").join("held-1.json"), "{}").unwrap();
        fs::write(spool.join("quarantine").join("quarantine-1.json"), "{}").unwrap();
        fs::write(spool.join("sent").join("sent-1.json"), "{}").unwrap();
        fs::write(spool.join("bounces").join("bounce-1.json"), "{").unwrap();

        let metrics = crate::smtp::queue_metrics(&spool, false).unwrap();
        assert_eq!(metrics.incoming_messages, 1);
        assert_eq!(metrics.active_messages, 2);
        assert_eq!(metrics.deferred_messages, 1);
        assert_eq!(metrics.held_messages, 1);
        assert_eq!(metrics.quarantined_messages, 1);
        assert_eq!(metrics.corrupt_messages, 2);
        assert_eq!(metrics.inbound_messages, 2);
        assert_eq!(metrics.delivery_attempts_last_hour, 2);
        assert!(!metrics.upstream_reachable);
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
    fn lpe_core_probe_urls_use_configured_delivery_base_url() {
        assert_eq!(
            lpe_health_probe_url("http://192.168.1.25:8080").unwrap(),
            "http://192.168.1.25:8080/health/live"
        );
        assert_eq!(
            lpe_bridge_probe_url("http://192.168.1.25:8080").unwrap(),
            "http://192.168.1.25:8080/internal/lpe-ct/recipient-verification"
        );
        assert_eq!(
            lpe_health_probe_url("https://lpe-core.example.test:8443/").unwrap(),
            "https://lpe-core.example.test:8443/health/live"
        );
    }

    #[test]
    fn accepted_domain_test_marks_domain_verified_once() {
        let mut domains = vec![AcceptedDomain {
            id: "domain-1".to_string(),
            domain: "example.test".to_string(),
            destination_server: "10.0.10.20:25".to_string(),
            verification_type: "dynamic".to_string(),
            rbl_checks: true,
            spf_checks: true,
            greylisting: true,
            accept_null_reverse_path: true,
            verified: false,
        }];

        assert!(mark_accepted_domain_verified(&mut domains, "domain-1"));
        assert!(domains[0].verified);
        assert!(!mark_accepted_domain_verified(&mut domains, "domain-1"));
        assert!(!mark_accepted_domain_verified(&mut domains, "missing"));
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
