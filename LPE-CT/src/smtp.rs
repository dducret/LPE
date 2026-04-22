use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use email_auth::{
    common::dns::{DnsError, DnsResolver, MxRecord},
    dkim::DkimResult,
    dmarc::Disposition as DmarcDisposition,
    spf::SpfResult,
    EmailAuthenticator,
};
use hickory_resolver::{proto::rr::RecordType, TokioResolver};
use lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, OutboundMessageHandoffRequest,
    OutboundMessageHandoffResponse, SignedIntegrationHeaders, TransportDeliveryStatus,
    TransportDsnReport, TransportRetryAdvice, TransportRouteDecision, TransportTechnicalStatus,
    TransportThrottleStatus, INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER,
    INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
};
use lpe_magika::{
    collect_mime_attachment_parts, extract_visible_text, parse_rfc822_header_value, Detector,
    ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, types::Json, PgPool, Row};
use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    convert::TryFrom,
    env, fs,
    hash::{Hash, Hasher},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{atomic::{AtomicU32, Ordering}, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream},
    process::Command,
    sync::OnceCell,
};
use tracing::{info, warn};

use crate::{integration_shared_secret, observability};

const INBOUND_DELIVERY_PATH: &str = "/internal/lpe-ct/inbound-deliveries";

static SMTP_ACTIVE_SESSIONS: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    primary_upstream: String,
    secondary_upstream: String,
    core_delivery_base_url: String,
    mutual_tls_required: bool,
    fallback_to_hold_queue: bool,
    drain_mode: bool,
    quarantine_enabled: bool,
    greylisting_enabled: bool,
    antivirus_enabled: bool,
    antivirus_fail_closed: bool,
    antivirus_provider_chain: Vec<String>,
    antivirus_providers: Vec<AntivirusProviderConfig>,
    bayespam_enabled: bool,
    bayespam_auto_learn: bool,
    bayespam_score_weight: f32,
    bayespam_min_token_length: u32,
    bayespam_max_tokens: u32,
    require_spf: bool,
    require_dkim_alignment: bool,
    require_dmarc_enforcement: bool,
    defer_on_auth_tempfail: bool,
    dnsbl_enabled: bool,
    dnsbl_zones: Vec<String>,
    reputation_enabled: bool,
    reputation_quarantine_threshold: i32,
    reputation_reject_threshold: i32,
    spam_quarantine_threshold: f32,
    spam_reject_threshold: f32,
    max_message_size_mb: u32,
    max_concurrent_sessions: u32,
    routing_rules: Vec<OutboundRoutingRule>,
    throttle_enabled: bool,
    throttle_rules: Vec<OutboundThrottleRule>,
    local_db_enabled: bool,
    local_db_url: Option<String>,
}

#[derive(Debug, Clone)]
struct OutboundRoutingRule {
    id: String,
    sender_domain: Option<String>,
    recipient_domain: Option<String>,
    relay_target: String,
}

#[derive(Debug, Clone)]
struct OutboundThrottleRule {
    id: String,
    scope: String,
    recipient_domain: Option<String>,
    sender_domain: Option<String>,
    max_messages: u32,
    window_seconds: u32,
    retry_after_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AuthSummary {
    spf: String,
    dkim: String,
    dmarc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DecisionTraceEntry {
    stage: String,
    outcome: String,
    detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuarantineMetadata {
    trace_id: String,
    direction: String,
    status: String,
    received_at: String,
    peer: String,
    helo: String,
    mail_from: String,
    rcpt_to: Vec<String>,
    subject: String,
    internet_message_id: Option<String>,
    spool_path: String,
    reason: Option<String>,
    spam_score: f32,
    security_score: f32,
    reputation_score: i32,
    dnsbl_hits: Vec<String>,
    auth_summary: AuthSummary,
    decision_trace: Vec<DecisionTraceEntry>,
    magika_summary: Option<String>,
    magika_decision: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QuarantineSummary {
    pub trace_id: String,
    pub queue: String,
    pub direction: String,
    pub status: String,
    pub received_at: String,
    pub mail_from: String,
    pub rcpt_to: Vec<String>,
    pub subject: String,
    pub reason: Option<String>,
    pub spam_score: f32,
    pub security_score: f32,
    pub reputation_score: i32,
    pub route_target: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TraceDetails {
    pub trace_id: String,
    pub queue: String,
    pub direction: String,
    pub status: String,
    pub received_at: String,
    pub peer: String,
    pub helo: String,
    pub mail_from: String,
    pub rcpt_to: Vec<String>,
    pub subject: String,
    pub internet_message_id: Option<String>,
    pub reason: Option<String>,
    pub remote_message_ref: Option<String>,
    pub spam_score: f32,
    pub security_score: f32,
    pub reputation_score: i32,
    pub technical_status: Option<TransportTechnicalStatus>,
    pub dsn: Option<TransportDsnReport>,
    pub route: Option<TransportRouteDecision>,
    pub throttle: Option<TransportThrottleStatus>,
    pub decision_trace: Vec<DecisionTraceEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TraceActionResult {
    pub trace_id: String,
    pub from_queue: String,
    pub to_queue: String,
    pub status: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueuedMessage {
    id: String,
    direction: String,
    received_at: String,
    peer: String,
    helo: String,
    mail_from: String,
    rcpt_to: Vec<String>,
    status: String,
    relay_error: Option<String>,
    magika_summary: Option<String>,
    magika_decision: Option<String>,
    #[serde(default)]
    spam_score: f32,
    #[serde(default)]
    security_score: f32,
    #[serde(default)]
    reputation_score: i32,
    #[serde(default)]
    dnsbl_hits: Vec<String>,
    #[serde(default)]
    auth_summary: AuthSummary,
    #[serde(default)]
    decision_trace: Vec<DecisionTraceEntry>,
    #[serde(default)]
    remote_message_ref: Option<String>,
    #[serde(default)]
    technical_status: Option<TransportTechnicalStatus>,
    #[serde(default)]
    dsn: Option<TransportDsnReport>,
    #[serde(default)]
    route: Option<TransportRouteDecision>,
    #[serde(default)]
    throttle: Option<TransportThrottleStatus>,
    #[serde(with = "base64_bytes")]
    data: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct TransportAuditEvent {
    timestamp: String,
    trace_id: String,
    direction: String,
    queue: String,
    status: String,
    peer: String,
    mail_from: String,
    rcpt_to: Vec<String>,
    subject: String,
    reason: Option<String>,
    route_target: Option<String>,
    remote_message_ref: Option<String>,
    spam_score: f32,
    security_score: f32,
    reputation_score: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ThrottleState {
    hits: Vec<u64>,
}

#[derive(Debug, Clone)]
struct AntivirusProviderConfig {
    id: String,
    display_name: String,
    command: String,
    args: Vec<String>,
    infected_markers: Vec<String>,
    suspicious_markers: Vec<String>,
    clean_markers: Vec<String>,
}

#[derive(Debug, Clone)]
struct AntivirusVerdict {
    action: FilterAction,
    reason: Option<String>,
    spam_score_delta: f32,
    security_score_delta: f32,
    decision_trace: Vec<DecisionTraceEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AntivirusProviderDecision {
    Clean,
    Suspicious,
    Infected,
}

#[derive(Debug, Clone)]
struct AntivirusScanTarget {
    root: PathBuf,
    attachment_count: usize,
}

#[derive(Debug, Clone)]
struct AntivirusScanOutcome {
    provider_id: String,
    provider_name: String,
    decision: AntivirusProviderDecision,
    summary: String,
}

#[derive(Debug, Clone)]
struct OutboundExecution {
    status: TransportDeliveryStatus,
    detail: Option<String>,
    remote_message_ref: Option<String>,
    retry: Option<TransportRetryAdvice>,
    dsn: Option<TransportDsnReport>,
    technical: Option<TransportTechnicalStatus>,
    route: Option<TransportRouteDecision>,
    throttle: Option<TransportThrottleStatus>,
}

#[derive(Debug, Clone)]
struct SmtpReply {
    code: u16,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InboundMagikaOutcome {
    Accept,
    Quarantine(String),
    Reject(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterAction {
    Accept,
    Quarantine,
    Reject,
    Defer,
}

#[derive(Debug, Clone)]
struct FilterVerdict {
    action: FilterAction,
    reason: Option<String>,
    spam_score: f32,
    security_score: f32,
    reputation_score: i32,
    dnsbl_hits: Vec<String>,
    auth_summary: AuthSummary,
    decision_trace: Vec<DecisionTraceEntry>,
}

#[derive(Debug, Clone)]
struct AuthenticationAssessment {
    spf: SpfDisposition,
    dkim: DkimDisposition,
    dkim_aligned: bool,
    spf_aligned: bool,
    dmarc: DmarcDisposition,
    from_domain: String,
    spf_domain: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpfDisposition {
    Pass,
    Fail,
    SoftFail,
    Neutral,
    None,
    TempError,
    PermError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DkimDisposition {
    Pass,
    Fail,
    None,
    TempFail,
    PermFail,
}

#[derive(Debug, Clone, Default)]
struct DnsblOutcome {
    hits: Vec<String>,
    tempfail_zones: Vec<String>,
}

impl AuthenticationAssessment {
    fn has_temporary_failure(&self) -> bool {
        matches!(self.spf, SpfDisposition::TempError)
            || matches!(self.dkim, DkimDisposition::TempFail)
            || matches!(self.dmarc, DmarcDisposition::TempFail)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct GreylistEntry {
    first_seen_unix: u64,
    release_after_unix: u64,
    pass_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ReputationStore {
    entries: HashMap<String, ReputationEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ReputationEntry {
    accepted: u32,
    quarantined: u32,
    rejected: u32,
    deferred: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct BayesCorpus {
    ham_messages: u32,
    spam_messages: u32,
    ham_tokens: HashMap<String, u32>,
    spam_tokens: HashMap<String, u32>,
}

#[derive(Debug, Clone)]
struct BayesOutcome {
    probability: f32,
    matched_tokens: usize,
    contribution: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BayesLabel {
    Ham,
    Spam,
}

const GREYLIST_DELAY_SECONDS: u64 = 90;

pub(crate) const SPOOL_QUEUES: [&str; 9] = [
    "incoming",
    "deferred",
    "quarantine",
    "held",
    "bounces",
    "sent",
    "outbound",
    "policy",
    "greylist",
];

pub(crate) const POLICY_ARTIFACTS: [&str; 4] = [
    "postgres: reputation_entries",
    "postgres: bayespam_corpora",
    "postgres: throttle_windows",
    "postgres: greylist_entries",
];

static LOCAL_DB_POOL: OnceCell<PgPool> = OnceCell::const_new();
static LOCAL_DB_POOL_URL: OnceLock<String> = OnceLock::new();
static LOCAL_DB_SCHEMA_READY: OnceCell<()> = OnceCell::const_new();

pub(crate) fn initialize_spool(spool_dir: &Path) -> Result<()> {
    for queue in SPOOL_QUEUES {
        fs::create_dir_all(spool_dir.join(queue))
            .with_context(|| format!("unable to create spool queue {queue}"))?;
    }
    Ok(())
}

pub(crate) async fn prepare_local_store(spool_dir: &Path, config: &RuntimeConfig) -> Result<()> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(());
    };
    migrate_legacy_policy_artifacts(spool_dir, pool).await
}

async fn ensure_local_db_schema(config: &RuntimeConfig) -> Result<Option<&'static PgPool>> {
    let Some(pool) = local_db_pool(config).await? else {
        return Ok(None);
    };

    LOCAL_DB_SCHEMA_READY
        .get_or_try_init(|| async {
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS greylist_entries (
                    entry_key TEXT PRIMARY KEY,
                    state JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS reputation_entries (
                    entry_key TEXT PRIMARY KEY,
                    state JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS bayespam_corpora (
                    corpus_key TEXT PRIMARY KEY,
                    corpus JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS throttle_windows (
                    rule_id TEXT NOT NULL,
                    bucket_key TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    state JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (rule_id, bucket_key)
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS quarantine_messages (
                    trace_id TEXT PRIMARY KEY,
                    direction TEXT NOT NULL,
                    status TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    peer TEXT NOT NULL,
                    helo TEXT NOT NULL,
                    mail_from TEXT NOT NULL,
                    rcpt_to JSONB NOT NULL,
                    subject TEXT NOT NULL,
                    internet_message_id TEXT,
                    spool_path TEXT NOT NULL,
                    reason TEXT,
                    spam_score REAL NOT NULL,
                    security_score REAL NOT NULL,
                    reputation_score INTEGER NOT NULL,
                    dnsbl_hits JSONB NOT NULL,
                    auth_summary JSONB NOT NULL,
                    decision_trace JSONB NOT NULL,
                    magika_summary TEXT,
                    magika_decision TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(Some(pool))
}

async fn local_db_pool(config: &RuntimeConfig) -> Result<Option<&'static PgPool>> {
    if !config.local_db_enabled {
        return Ok(None);
    }

    let database_url = config.local_db_url.as_deref().ok_or_else(|| {
        anyhow!("LPE_CT_LOCAL_DB_URL must be set when LPE_CT_LOCAL_DB_ENABLED=true")
    })?;

    if let Some(initialized_url) = LOCAL_DB_POOL_URL.get() {
        if initialized_url != database_url {
            return Err(anyhow!(
                "LPE_CT_LOCAL_DB_URL changed after pool initialization; restart LPE-CT to switch databases"
            ));
        }
    }

    let pool = LOCAL_DB_POOL
        .get_or_try_init(|| async move {
            let pool = PgPoolOptions::new()
                .max_connections(8)
                .connect(database_url)
                .await
                .with_context(|| "unable to connect to the dedicated LPE-CT PostgreSQL store")?;
            let _ = LOCAL_DB_POOL_URL.set(database_url.to_string());
            Ok::<PgPool, anyhow::Error>(pool)
        })
        .await?;

    Ok(Some(pool))
}

async fn migrate_legacy_policy_artifacts(spool_dir: &Path, pool: &PgPool) -> Result<()> {
    migrate_legacy_reputation_store(spool_dir, pool).await?;
    migrate_legacy_bayespam_corpus(spool_dir, pool).await?;
    migrate_legacy_greylist_entries(spool_dir, pool).await?;
    Ok(())
}

async fn migrate_legacy_reputation_store(spool_dir: &Path, pool: &PgPool) -> Result<()> {
    let legacy_path = spool_dir.join("policy").join("reputation.json");
    if !legacy_path.exists() {
        return Ok(());
    }

    let existing_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM reputation_entries")
        .fetch_one(pool)
        .await?;
    if existing_count > 0 {
        return Ok(());
    }

    let store: ReputationStore = serde_json::from_str(&fs::read_to_string(&legacy_path)?)?;
    for (entry_key, state) in store.entries {
        sqlx::query(
            r#"
            INSERT INTO reputation_entries (entry_key, state, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (entry_key) DO UPDATE SET
                state = EXCLUDED.state,
                updated_at = NOW()
            "#,
        )
        .bind(entry_key)
        .bind(Json(state))
        .execute(pool)
        .await?;
    }

    Ok(())
}

async fn migrate_legacy_bayespam_corpus(spool_dir: &Path, pool: &PgPool) -> Result<()> {
    let legacy_path = spool_dir.join("policy").join("bayespam.json");
    if !legacy_path.exists() {
        return Ok(());
    }

    let existing = sqlx::query("SELECT corpus FROM bayespam_corpora WHERE corpus_key = $1")
        .bind("default")
        .fetch_optional(pool)
        .await?;
    if existing.is_some() {
        return Ok(());
    }

    let corpus: BayesCorpus = serde_json::from_str(&fs::read_to_string(&legacy_path)?)?;
    sqlx::query(
        r#"
        INSERT INTO bayespam_corpora (corpus_key, corpus, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (corpus_key) DO UPDATE SET
            corpus = EXCLUDED.corpus,
            updated_at = NOW()
        "#,
    )
    .bind("default")
    .bind(Json(corpus))
    .execute(pool)
    .await?;

    Ok(())
}

async fn migrate_legacy_greylist_entries(spool_dir: &Path, pool: &PgPool) -> Result<()> {
    let legacy_dir = spool_dir.join("greylist");
    if !legacy_dir.is_dir() {
        return Ok(());
    }

    let existing_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM greylist_entries")
        .fetch_one(pool)
        .await?;
    if existing_count > 0 {
        return Ok(());
    }

    for entry in fs::read_dir(&legacy_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(key) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        let state: GreylistEntry = serde_json::from_str(&fs::read_to_string(&path)?)?;
        sqlx::query(
            r#"
            INSERT INTO greylist_entries (entry_key, state, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (entry_key) DO UPDATE SET
                state = EXCLUDED.state,
                updated_at = NOW()
            "#,
        )
        .bind(key)
        .bind(Json(state))
        .execute(pool)
        .await?;
    }

    Ok(())
}

pub(crate) fn queue_metrics(
    spool_dir: &Path,
    upstream_reachable: bool,
) -> Result<super::QueueMetrics> {
    Ok(super::QueueMetrics {
        inbound_messages: count_queue(spool_dir, "incoming")? + count_queue(spool_dir, "sent")?,
        deferred_messages: count_queue(spool_dir, "deferred")?,
        quarantined_messages: count_queue(spool_dir, "quarantine")?,
        held_messages: count_queue(spool_dir, "held")?,
        delivery_attempts_last_hour: count_queue(spool_dir, "sent")?
            + count_queue(spool_dir, "deferred")?,
        upstream_reachable,
    })
}

pub(crate) async fn run_smtp_listener(
    bind_address: String,
    state_file: PathBuf,
    spool_dir: PathBuf,
) -> Result<()> {
    let listener = TcpListener::bind(&bind_address)
        .await
        .with_context(|| format!("unable to bind SMTP listener on {bind_address}"))?;
    info!("lpe-ct smtp listener active on {bind_address}");

    loop {
        let (stream, peer) = listener.accept().await?;
        let max_concurrent_sessions = runtime_config(&state_file)
            .map(|config| config.max_concurrent_sessions.max(1))
            .unwrap_or(250);
        let current = SMTP_ACTIVE_SESSIONS.fetch_add(1, Ordering::SeqCst) + 1;
        observability::set_active_smtp_sessions(current);
        if current > max_concurrent_sessions {
            SMTP_ACTIVE_SESSIONS.fetch_sub(1, Ordering::SeqCst);
            observability::set_active_smtp_sessions(SMTP_ACTIVE_SESSIONS.load(Ordering::SeqCst));
            observability::record_smtp_backpressure();
            tokio::spawn(async move {
                let (_, mut writer) = stream.into_split();
                let _ = write_smtp(
                    &mut writer,
                    "421 too many concurrent SMTP sessions; try again later",
                )
                .await;
            });
            continue;
        }
        let state_file = state_file.clone();
        let spool_dir = spool_dir.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_smtp_session(stream, peer, state_file, spool_dir).await {
                observability::record_smtp_session("failed");
                warn!(peer = %peer, error = %error, "smtp session failed");
            }
            let remaining = SMTP_ACTIVE_SESSIONS.fetch_sub(1, Ordering::SeqCst) - 1;
            observability::set_active_smtp_sessions(remaining);
        });
    }
}

pub(crate) fn runtime_config_from_dashboard(dashboard: &super::DashboardState) -> RuntimeConfig {
    RuntimeConfig {
        primary_upstream: dashboard.relay.primary_upstream.clone(),
        secondary_upstream: dashboard.relay.secondary_upstream.clone(),
        core_delivery_base_url: dashboard.relay.core_delivery_base_url.clone(),
        mutual_tls_required: dashboard.relay.mutual_tls_required,
        fallback_to_hold_queue: dashboard.relay.fallback_to_hold_queue,
        drain_mode: dashboard.policies.drain_mode,
        quarantine_enabled: dashboard.policies.quarantine_enabled,
        greylisting_enabled: dashboard.policies.greylisting_enabled,
        antivirus_enabled: dashboard.policies.antivirus_enabled,
        antivirus_fail_closed: dashboard.policies.antivirus_fail_closed,
        antivirus_provider_chain: dashboard.policies.antivirus_provider_chain.clone(),
        antivirus_providers: load_antivirus_providers(&dashboard.policies.antivirus_provider_chain),
        bayespam_enabled: dashboard.policies.bayespam_enabled,
        bayespam_auto_learn: dashboard.policies.bayespam_auto_learn,
        bayespam_score_weight: dashboard.policies.bayespam_score_weight,
        bayespam_min_token_length: dashboard.policies.bayespam_min_token_length,
        bayespam_max_tokens: dashboard.policies.bayespam_max_tokens,
        require_spf: dashboard.policies.require_spf,
        require_dkim_alignment: dashboard.policies.require_dkim_alignment,
        require_dmarc_enforcement: dashboard.policies.require_dmarc_enforcement,
        defer_on_auth_tempfail: dashboard.policies.defer_on_auth_tempfail,
        dnsbl_enabled: dashboard.policies.dnsbl_enabled,
        dnsbl_zones: dashboard.policies.dnsbl_zones.clone(),
        reputation_enabled: dashboard.policies.reputation_enabled,
        reputation_quarantine_threshold: dashboard.policies.reputation_quarantine_threshold,
        reputation_reject_threshold: dashboard.policies.reputation_reject_threshold,
        spam_quarantine_threshold: dashboard.policies.spam_quarantine_threshold,
        spam_reject_threshold: dashboard.policies.spam_reject_threshold,
        max_message_size_mb: dashboard.policies.max_message_size_mb,
        max_concurrent_sessions: dashboard.network.max_concurrent_sessions.max(1),
        routing_rules: dashboard
            .routing
            .rules
            .iter()
            .map(|rule| OutboundRoutingRule {
                id: rule.id.clone(),
                sender_domain: rule.sender_domain.clone(),
                recipient_domain: rule.recipient_domain.clone(),
                relay_target: rule.relay_target.clone(),
            })
            .collect(),
        throttle_enabled: dashboard.throttling.enabled,
        throttle_rules: dashboard
            .throttling
            .rules
            .iter()
            .map(|rule| OutboundThrottleRule {
                id: rule.id.clone(),
                scope: rule.scope.clone(),
                recipient_domain: rule.recipient_domain.clone(),
                sender_domain: rule.sender_domain.clone(),
                max_messages: rule.max_messages,
                window_seconds: rule.window_seconds,
                retry_after_seconds: rule.retry_after_seconds,
            })
            .collect(),
        local_db_enabled: dashboard.local_data_stores.dedicated_postgres.enabled,
        local_db_url: env::var("LPE_CT_LOCAL_DB_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
    }
}

fn load_antivirus_providers(provider_chain: &[String]) -> Vec<AntivirusProviderConfig> {
    provider_chain
        .iter()
        .filter_map(|provider_id| antivirus_provider_from_env(provider_id))
        .collect()
}

fn antivirus_provider_from_env(provider_id: &str) -> Option<AntivirusProviderConfig> {
    let normalized = provider_id.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }

    if normalized == "takeri" {
        return Some(AntivirusProviderConfig {
            id: normalized,
            display_name: "takeri".to_string(),
            command: env::var("LPE_CT_ANTIVIRUS_TAKERI_BIN")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "/opt/lpe-ct/bin/Shuhari-CyberForge-CLI".to_string()),
            args: env::var("LPE_CT_ANTIVIRUS_TAKERI_ARGS")
                .ok()
                .map(|value| parse_csv_env(&value))
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| vec!["takeri".to_string(), "scan".to_string()]),
            infected_markers: vec![
                "status: infected".to_string(),
                "infected files detected".to_string(),
                "infected files:".to_string(),
                "critical: malware detected".to_string(),
            ],
            suspicious_markers: vec![
                "status: suspicious".to_string(),
                "suspicious files:".to_string(),
            ],
            clean_markers: vec![
                "status: clean".to_string(),
                "no threats detected".to_string(),
            ],
        });
    }

    let env_key = normalized.replace('-', "_").to_ascii_uppercase();
    let command = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_BIN"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())?;
    let args = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_ARGS"))
        .ok()
        .map(|value| parse_csv_env(&value))
        .unwrap_or_default();
    let infected_markers = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_INFECTED_MARKERS"))
        .ok()
        .map(|value| parse_csv_env(&value))
        .unwrap_or_else(|| vec!["infected".to_string(), "malware".to_string()]);
    let suspicious_markers = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_SUSPICIOUS_MARKERS"))
        .ok()
        .map(|value| parse_csv_env(&value))
        .unwrap_or_else(|| vec!["suspicious".to_string()]);
    let clean_markers = env::var(format!("LPE_CT_ANTIVIRUS_{env_key}_CLEAN_MARKERS"))
        .ok()
        .map(|value| parse_csv_env(&value))
        .unwrap_or_else(|| vec!["clean".to_string(), "ok".to_string()]);

    Some(AntivirusProviderConfig {
        id: normalized.clone(),
        display_name: normalized,
        command,
        args,
        infected_markers,
        suspicious_markers,
        clean_markers,
    })
}

fn parse_csv_env(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

pub(crate) async fn process_outbound_handoff(
    spool_dir: &Path,
    config: &RuntimeConfig,
    payload: OutboundMessageHandoffRequest,
) -> Result<OutboundMessageHandoffResponse> {
    let message_id = payload.message_id;
    let internet_message_id = payload.internet_message_id.clone();
    let route = resolve_outbound_route(config, &payload);
    let mut message = QueuedMessage {
        id: format!("lpe-ct-out-{}", payload.queue_id),
        direction: "outbound".to_string(),
        received_at: current_timestamp(),
        peer: "lpe-core".to_string(),
        helo: "lpe-core".to_string(),
        mail_from: payload.from_address.clone(),
        rcpt_to: payload.envelope_recipients(),
        status: "outbound".to_string(),
        relay_error: None,
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: vec![DecisionTraceEntry {
            stage: "outbound-handoff".to_string(),
            outcome: "accepted".to_string(),
            detail: "message received from LPE core for outbound relay".to_string(),
        }],
        remote_message_ref: None,
        technical_status: None,
        dsn: None,
        route: Some(route.clone()),
        throttle: None,
        data: compose_rfc822_message(&payload),
    };
    message.decision_trace.push(DecisionTraceEntry {
        stage: "protocol".to_string(),
        outcome: "queued".to_string(),
        detail: format!(
            "outbound handoff contains {} envelope recipient(s) and attempt_count={}",
            message.rcpt_to.len(),
            payload.attempt_count
        ),
    });
    if let Some(last_attempt_error) = payload
        .last_attempt_error
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        message.decision_trace.push(DecisionTraceEntry {
            stage: "retry-context".to_string(),
            outcome: "previous-failure".to_string(),
            detail: last_attempt_error.to_string(),
        });
    }
    message.decision_trace.push(DecisionTraceEntry {
        stage: "routing".to_string(),
        outcome: "selected".to_string(),
        detail: format!(
            "relay_target={} rule_id={}",
            route.relay_target.as_deref().unwrap_or(""),
            route.rule_id.as_deref().unwrap_or("default")
        ),
    });
    match score_bayespam(
        spool_dir,
        config,
        &payload.subject,
        &payload.body_text,
        &payload.from_address,
        "lpe-core",
    )
    .await?
    {
        Some(outcome) => {
            message.spam_score += outcome.contribution;
            message.decision_trace.push(DecisionTraceEntry {
                stage: "bayespam".to_string(),
                outcome: if outcome.probability >= 0.90 {
                    "spam"
                } else if outcome.probability >= 0.70 {
                    "suspect"
                } else {
                    "ham"
                }
                .to_string(),
                detail: format!(
                    "bayespam probability {:.3} using {} learned tokens (contribution={:.2})",
                    outcome.probability, outcome.matched_tokens, outcome.contribution
                ),
            });
        }
        None if config.bayespam_enabled => {
            message.decision_trace.push(DecisionTraceEntry {
                stage: "bayespam".to_string(),
                outcome: "skipped".to_string(),
                detail: "bayespam corpus is not trained enough for scoring yet".to_string(),
            });
        }
        None => {
            message.decision_trace.push(DecisionTraceEntry {
                stage: "bayespam".to_string(),
                outcome: "disabled".to_string(),
                detail: "bayespam disabled by local policy".to_string(),
            });
        }
    }
    let antivirus_verdict = evaluate_antivirus_policy(config, "outbound", &message.data).await?;
    message.spam_score += antivirus_verdict.spam_score_delta;
    message.security_score += antivirus_verdict.security_score_delta;
    message
        .decision_trace
        .extend(antivirus_verdict.decision_trace.clone());
    if antivirus_verdict.action == FilterAction::Quarantine {
        message.status = "quarantined".to_string();
        message.relay_error = antivirus_verdict.reason;
        message.decision_trace.push(DecisionTraceEntry {
            stage: "outbound-policy".to_string(),
            outcome: "quarantine".to_string(),
            detail: message
                .relay_error
                .clone()
                .unwrap_or_else(|| "antivirus provider requested quarantine".to_string()),
        });
        move_message(spool_dir, &message, "outbound", "quarantine").await?;
        persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
        let _ = append_transport_audit(spool_dir, "quarantine", &message);
        observability::record_security_event("outbound_quarantine");
        return Ok(OutboundMessageHandoffResponse {
            queue_id: payload.queue_id,
            status: TransportDeliveryStatus::Quarantined,
            trace_id: message.id,
            detail: message.relay_error,
            remote_message_ref: None,
            retry: None,
            dsn: None,
            technical: None,
            route: Some(route),
            throttle: None,
        });
    }
    message.decision_trace.push(DecisionTraceEntry {
        stage: "final-score".to_string(),
        outcome: "calculated".to_string(),
        detail: format!(
            "outbound spam_score={:.2} security_score={:.2}",
            message.spam_score, message.security_score
        ),
    });

    persist_message(spool_dir, "outbound", &message).await?;

    if config.quarantine_enabled && should_quarantine(&message.data) {
        message.status = "quarantined".to_string();
        message.spam_score = config.spam_quarantine_threshold.max(1.0);
        message.decision_trace.push(DecisionTraceEntry {
            stage: "outbound-policy".to_string(),
            outcome: "quarantine".to_string(),
            detail: "message matched local quarantine policy".to_string(),
        });
        move_message(spool_dir, &message, "outbound", "quarantine").await?;
        persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
        let _ = append_transport_audit(spool_dir, "quarantine", &message);
        observability::record_security_event("outbound_quarantine");
        info!(
            trace_id = %message.id,
            message_id = %message_id,
            internet_message_id = internet_message_id.as_deref().unwrap_or(""),
            status = "quarantined",
            "outbound handoff quarantined by lpe-ct policy"
        );
        return Ok(OutboundMessageHandoffResponse {
            queue_id: payload.queue_id,
            status: TransportDeliveryStatus::Quarantined,
            trace_id: message.id,
            detail: Some("message matched quarantine policy".to_string()),
            remote_message_ref: None,
            retry: None,
            dsn: None,
            technical: None,
            route: Some(route),
            throttle: None,
        });
    }

    if config.quarantine_enabled && message.spam_score >= config.spam_quarantine_threshold {
        message.status = "quarantined".to_string();
        message.relay_error = Some(format!(
            "bayespam score {:.2} reached quarantine threshold {:.2}",
            message.spam_score, config.spam_quarantine_threshold
        ));
        message.decision_trace.push(DecisionTraceEntry {
            stage: "outbound-policy".to_string(),
            outcome: "quarantine".to_string(),
            detail: message.relay_error.clone().unwrap_or_default(),
        });
        move_message(spool_dir, &message, "outbound", "quarantine").await?;
        persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
        let _ = append_transport_audit(spool_dir, "quarantine", &message);
        observability::record_security_event("outbound_quarantine");
        return Ok(OutboundMessageHandoffResponse {
            queue_id: payload.queue_id,
            status: TransportDeliveryStatus::Quarantined,
            trace_id: message.id,
            detail: message.relay_error,
            remote_message_ref: None,
            retry: None,
            dsn: None,
            technical: None,
            route: Some(route),
            throttle: None,
        });
    }

    if let Some(throttle) = evaluate_outbound_throttle(spool_dir, config, &payload).await? {
        message.status = "deferred".to_string();
        message.throttle = Some(throttle.clone());
        message.route = Some(route.clone());
        message.technical_status = Some(TransportTechnicalStatus {
            phase: "throttle".to_string(),
            smtp_code: Some(451),
            enhanced_code: Some("4.7.1".to_string()),
            remote_host: route.relay_target.clone(),
            detail: Some(format!("throttled by {}", throttle.scope)),
        });
        message.relay_error = Some("message throttled before outbound relay".to_string());
        message.decision_trace.push(DecisionTraceEntry {
            stage: "outbound-throttle".to_string(),
            outcome: "deferred".to_string(),
            detail: format!(
                "scope={} key={} retry_after={}s",
                throttle.scope, throttle.key, throttle.retry_after_seconds
            ),
        });
        move_message(spool_dir, &message, "outbound", "deferred").await?;
        let _ = append_transport_audit(spool_dir, "deferred", &message);
        info!(
            trace_id = %message.id,
            message_id = %message_id,
            internet_message_id = internet_message_id.as_deref().unwrap_or(""),
            status = "deferred",
            throttle_scope = %throttle.scope,
            "outbound handoff deferred by lpe-ct throttle"
        );
        return Ok(OutboundMessageHandoffResponse {
            queue_id: payload.queue_id,
            status: TransportDeliveryStatus::Deferred,
            trace_id: message.id,
            detail: Some("message throttled before outbound relay".to_string()),
            remote_message_ref: None,
            retry: Some(TransportRetryAdvice {
                retry_after_seconds: throttle.retry_after_seconds,
                policy: "throttle".to_string(),
                reason: Some(format!("{} {}", throttle.scope, throttle.key)),
            }),
            dsn: Some(TransportDsnReport {
                action: "delayed".to_string(),
                status: "4.7.1".to_string(),
                diagnostic_code: Some("smtp; 451 4.7.1 locally throttled".to_string()),
                remote_mta: route.relay_target.clone(),
            }),
            technical: message.technical_status.clone(),
            route: Some(route),
            throttle: Some(throttle),
        });
    }

    let execution = relay_message(
        config,
        &message,
        &route,
        payload.attempt_count,
        payload.last_attempt_error.as_deref(),
    )
    .await;
    message.status = execution
        .route
        .as_ref()
        .map(|decision| decision.queue.clone())
        .unwrap_or_else(|| default_queue_for_status(&execution.status).to_string());
    message.relay_error = execution.detail.clone();
    message.remote_message_ref = execution.remote_message_ref.clone();
    message.technical_status = execution.technical.clone();
    message.dsn = execution.dsn.clone();
    message.route = execution.route.clone().or_else(|| Some(route.clone()));
    message.throttle = execution.throttle.clone();
    message.decision_trace.push(DecisionTraceEntry {
        stage: "outbound-relay".to_string(),
        outcome: execution.status.as_str().to_string(),
        detail: execution
            .detail
            .clone()
            .unwrap_or_else(|| "outbound relay completed".to_string()),
    });
    let destination = default_queue_for_status(&execution.status);
    if destination == "bounces" {
        persist_message(spool_dir, "bounces", &message).await?;
        move_message(spool_dir, &message, "outbound", "held").await?;
    } else {
        move_message(spool_dir, &message, "outbound", destination).await?;
    }
    if destination == "quarantine" {
        persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
    }
    let _ = append_transport_audit(spool_dir, destination, &message);
    if matches!(
        execution.status,
        TransportDeliveryStatus::Quarantined
            | TransportDeliveryStatus::Bounced
            | TransportDeliveryStatus::Failed
    ) {
        observability::record_security_event(match execution.status {
            TransportDeliveryStatus::Quarantined => "outbound_quarantine",
            TransportDeliveryStatus::Bounced => "outbound_bounce",
            TransportDeliveryStatus::Failed => "outbound_failure",
            _ => "outbound_transport",
        });
    }
    info!(
        trace_id = %message.id,
        message_id = %message_id,
        internet_message_id = internet_message_id.as_deref().unwrap_or(""),
        status = execution.status.as_str(),
        relay_target = route.relay_target.as_deref().unwrap_or(""),
        "outbound handoff completed"
    );

    Ok(OutboundMessageHandoffResponse {
        queue_id: payload.queue_id,
        status: execution.status,
        trace_id: message.id,
        detail: execution.detail,
        remote_message_ref: execution.remote_message_ref,
        retry: execution.retry,
        dsn: execution.dsn,
        technical: execution.technical,
        route: execution.route.or(Some(route)),
        throttle: execution.throttle,
    })
}

fn resolve_outbound_route(
    config: &RuntimeConfig,
    payload: &OutboundMessageHandoffRequest,
) -> TransportRouteDecision {
    let sender_domain = domain_part(&payload.from_address);
    let recipient_domains = payload
        .envelope_recipients()
        .into_iter()
        .filter_map(|address| domain_part(&address))
        .collect::<Vec<_>>();

    let matched = config.routing_rules.iter().find(|rule| {
        matches_domain(rule.sender_domain.as_deref(), sender_domain.as_deref())
            && matches_any_domain(rule.recipient_domain.as_deref(), &recipient_domains)
    });

    if let Some(rule) = matched {
        return TransportRouteDecision {
            rule_id: Some(rule.id.clone()),
            relay_target: Some(rule.relay_target.clone()),
            queue: "outbound".to_string(),
        };
    }

    TransportRouteDecision {
        rule_id: None,
        relay_target: if !config.primary_upstream.trim().is_empty() {
            Some(config.primary_upstream.clone())
        } else if !config.secondary_upstream.trim().is_empty() {
            Some(config.secondary_upstream.clone())
        } else {
            None
        },
        queue: "outbound".to_string(),
    }
}

async fn evaluate_outbound_throttle(
    spool_dir: &Path,
    config: &RuntimeConfig,
    payload: &OutboundMessageHandoffRequest,
) -> Result<Option<TransportThrottleStatus>> {
    if !config.throttle_enabled {
        return Ok(None);
    }

    let sender_domain = domain_part(&payload.from_address);
    let recipient_domains = payload
        .envelope_recipients()
        .into_iter()
        .filter_map(|address| domain_part(&address))
        .collect::<Vec<_>>();

    for rule in &config.throttle_rules {
        if !matches_domain(rule.sender_domain.as_deref(), sender_domain.as_deref())
            || !matches_any_domain(rule.recipient_domain.as_deref(), &recipient_domains)
        {
            continue;
        }

        let key = match rule.scope.as_str() {
            "sender-domain" => sender_domain
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            _ => recipient_domains
                .first()
                .cloned()
                .unwrap_or_else(|| "unknown".to_string()),
        };
        let mut state = if let Some(pool) = ensure_local_db_schema(config).await? {
            let row = sqlx::query(
                "SELECT state FROM throttle_windows WHERE rule_id = $1 AND bucket_key = $2",
            )
            .bind(&rule.id)
            .bind(&key)
            .fetch_optional(pool)
            .await?;
            row.map(|row| row.try_get::<Json<ThrottleState>, _>("state"))
                .transpose()?
                .map(|value| value.0)
                .unwrap_or_default()
        } else {
            let state_path = spool_dir.join("policy").join(format!(
                "throttle-{}.json",
                stable_key_id(&(rule.id.clone(), key.clone()))
            ));
            if state_path.exists() {
                serde_json::from_str::<ThrottleState>(&fs::read_to_string(&state_path)?)?
            } else {
                ThrottleState::default()
            }
        };
        let now = unix_now();
        state
            .hits
            .retain(|timestamp| now.saturating_sub(*timestamp) < rule.window_seconds as u64);
        if state.hits.len() >= rule.max_messages as usize {
            return Ok(Some(TransportThrottleStatus {
                scope: rule.scope.clone(),
                key,
                limit: rule.max_messages,
                window_seconds: rule.window_seconds,
                retry_after_seconds: rule.retry_after_seconds.max(1),
            }));
        }

        state.hits.push(now);
        if let Some(pool) = ensure_local_db_schema(config).await? {
            sqlx::query(
                r#"
                INSERT INTO throttle_windows (rule_id, bucket_key, scope, state, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (rule_id, bucket_key) DO UPDATE SET
                    scope = EXCLUDED.scope,
                    state = EXCLUDED.state,
                    updated_at = NOW()
                "#,
            )
            .bind(&rule.id)
            .bind(&key)
            .bind(&rule.scope)
            .bind(Json(&state))
            .execute(pool)
            .await?;
        } else {
            let state_path = spool_dir.join("policy").join(format!(
                "throttle-{}.json",
                stable_key_id(&(rule.id.clone(), key.clone()))
            ));
            fs::write(&state_path, serde_json::to_string_pretty(&state)?)?;
        }
    }

    Ok(None)
}

fn default_queue_for_status(status: &TransportDeliveryStatus) -> &'static str {
    match status {
        TransportDeliveryStatus::Relayed => "sent",
        TransportDeliveryStatus::Deferred => "deferred",
        TransportDeliveryStatus::Quarantined => "quarantine",
        TransportDeliveryStatus::Bounced => "bounces",
        TransportDeliveryStatus::Queued => "outbound",
        TransportDeliveryStatus::Failed => "held",
    }
}

fn domain_part(address: &str) -> Option<String> {
    address
        .rsplit_once('@')
        .map(|(_, domain)| domain.trim().to_ascii_lowercase())
        .filter(|domain| !domain.is_empty())
}

fn matches_domain(expected: Option<&str>, actual: Option<&str>) -> bool {
    match expected.map(|value| value.trim().to_ascii_lowercase()) {
        Some(expected) if !expected.is_empty() => actual == Some(expected.as_str()),
        _ => true,
    }
}

fn matches_any_domain(expected: Option<&str>, actual: &[String]) -> bool {
    match expected.map(|value| value.trim().to_ascii_lowercase()) {
        Some(expected) if !expected.is_empty() => actual.iter().any(|value| value == &expected),
        _ => true,
    }
}

async fn handle_smtp_session(
    stream: TcpStream,
    peer: SocketAddr,
    state_file: PathBuf,
    spool_dir: PathBuf,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut helo = String::new();
    let mut mail_from = String::new();
    let mut rcpt_to = Vec::new();

    if let Some(role) = crate::ha_non_active_role_for_traffic()? {
        write_smtp(
            &mut writer,
            &format!("421 node role {role} is not accepting SMTP traffic"),
        )
        .await?;
        observability::record_smtp_session("ha-blocked");
        return Ok(());
    }

    write_smtp(&mut writer, "220 LPE-CT ESMTP ready").await?;
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }

        let command = line.trim_end_matches(['\r', '\n']).to_string();
        let upper = command.to_ascii_uppercase();
        if upper.starts_with("EHLO ") || upper.starts_with("HELO ") {
            helo = command[5.min(command.len())..].trim().to_string();
            write_smtp(&mut writer, "250-LPE-CT").await?;
            write_smtp(&mut writer, "250 SIZE").await?;
        } else if upper.starts_with("MAIL FROM:") {
            mail_from = command[10..].trim().trim_matches(['<', '>']).to_string();
            rcpt_to.clear();
            write_smtp(&mut writer, "250 sender accepted").await?;
        } else if upper.starts_with("RCPT TO:") {
            if mail_from.is_empty() {
                write_smtp(&mut writer, "503 send MAIL FROM first").await?;
                continue;
            }
            rcpt_to.push(command[8..].trim().trim_matches(['<', '>']).to_string());
            write_smtp(&mut writer, "250 recipient accepted").await?;
        } else if upper == "DATA" {
            if mail_from.is_empty() || rcpt_to.is_empty() {
                write_smtp(&mut writer, "503 sender and recipient required").await?;
                continue;
            }
            let config = runtime_config(&state_file)?;
            write_smtp(&mut writer, "354 end with <CRLF>.<CRLF>").await?;
            let data = read_smtp_data(&mut reader, config.max_message_size_mb).await?;
            let message = receive_message(
                &spool_dir,
                &config,
                peer.to_string(),
                helo.clone(),
                mail_from.clone(),
                rcpt_to.clone(),
                data,
            )
            .await?;
            if message.status == "rejected" {
                write_smtp(
                    &mut writer,
                    &format!(
                        "554 message rejected by perimeter policy (trace {})",
                        message.id
                    ),
                )
                .await?;
            } else if message.status == "deferred" {
                write_smtp(
                    &mut writer,
                    &format!(
                        "451 message temporarily deferred by perimeter policy (trace {})",
                        message.id
                    ),
                )
                .await?;
            } else if message.status == "quarantined" {
                write_smtp(&mut writer, &format!("250 quarantined as {}", message.id)).await?;
                return Ok(());
            } else {
                write_smtp(&mut writer, &format!("250 queued as {}", message.id)).await?;
            }
            if message.status == "rejected" {
                return Ok(());
            }
            mail_from.clear();
            rcpt_to.clear();
        } else if upper == "RSET" {
            mail_from.clear();
            rcpt_to.clear();
            write_smtp(&mut writer, "250 reset").await?;
        } else if upper == "NOOP" {
            write_smtp(&mut writer, "250 ok").await?;
        } else if upper == "QUIT" {
            write_smtp(&mut writer, "221 bye").await?;
            return Ok(());
        } else {
            write_smtp(&mut writer, "502 command not implemented").await?;
        }
    }
}

async fn receive_message(
    spool_dir: &Path,
    config: &RuntimeConfig,
    peer: String,
    helo: String,
    mail_from: String,
    rcpt_to: Vec<String>,
    data: Vec<u8>,
) -> Result<QueuedMessage> {
    receive_message_with_validator(
        &Validator::from_env(),
        spool_dir,
        config,
        peer,
        helo,
        mail_from,
        rcpt_to,
        data,
    )
    .await
}

async fn receive_message_with_validator<D: Detector>(
    validator: &Validator<D>,
    spool_dir: &Path,
    config: &RuntimeConfig,
    peer: String,
    helo: String,
    mail_from: String,
    rcpt_to: Vec<String>,
    data: Vec<u8>,
) -> Result<QueuedMessage> {
    let mut message = QueuedMessage {
        id: message_id("in"),
        direction: "inbound".to_string(),
        received_at: current_timestamp(),
        peer,
        helo,
        mail_from,
        rcpt_to,
        status: "incoming".to_string(),
        relay_error: None,
        magika_summary: None,
        magika_decision: None,
        spam_score: 0.0,
        security_score: 0.0,
        reputation_score: 0,
        dnsbl_hits: Vec::new(),
        auth_summary: AuthSummary::default(),
        decision_trace: vec![DecisionTraceEntry {
            stage: "ingress".to_string(),
            outcome: "accepted".to_string(),
            detail: "message accepted by SMTP edge and persisted to the incoming spool".to_string(),
        }],
        remote_message_ref: None,
        technical_status: None,
        dsn: None,
        route: None,
        throttle: None,
        data,
    };

    persist_message(spool_dir, "incoming", &message).await?;
    message.decision_trace.push(DecisionTraceEntry {
        stage: "protocol".to_string(),
        outcome: "smtp-envelope".to_string(),
        detail: format!(
            "peer={} helo={} mail_from={} rcpt_count={}",
            message.peer,
            message.helo,
            message.mail_from,
            message.rcpt_to.len()
        ),
    });

    if config.drain_mode {
        message.status = "held".to_string();
        message.decision_trace.push(DecisionTraceEntry {
            stage: "drain-mode".to_string(),
            outcome: "held".to_string(),
            detail: "drain mode is enabled on the sorting center".to_string(),
        });
        move_message(spool_dir, &message, "incoming", "held").await?;
        let _ = append_transport_audit(spool_dir, "held", &message);
        return Ok(message);
    }

    match classify_inbound_message(validator, &message.data) {
        Ok(InboundMagikaOutcome::Accept) => {}
        Ok(InboundMagikaOutcome::Quarantine(reason)) => {
            observability::record_security_event("magika_quarantine");
            message.status = "quarantined".to_string();
            message.magika_decision = Some("quarantine".to_string());
            message.magika_summary = Some(reason);
            message.security_score += 5.0;
            message.decision_trace.push(DecisionTraceEntry {
                stage: "magika".to_string(),
                outcome: "quarantine".to_string(),
                detail: message.magika_summary.clone().unwrap_or_default(),
            });
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, "quarantine", &message);
            info!(
                trace_id = %message.id,
                status = %message.status,
                "inbound message quarantined by Magika"
            );
            return Ok(message);
        }
        Ok(InboundMagikaOutcome::Reject(reason)) => {
            observability::record_security_event("magika_reject");
            message.status = "rejected".to_string();
            message.magika_decision = Some("reject".to_string());
            message.magika_summary = Some(reason);
            message.security_score += 8.0;
            message.decision_trace.push(DecisionTraceEntry {
                stage: "magika".to_string(),
                outcome: "reject".to_string(),
                detail: message.magika_summary.clone().unwrap_or_default(),
            });
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, "quarantine", &message);
            info!(
                trace_id = %message.id,
                status = %message.status,
                "inbound message rejected by Magika"
            );
            return Ok(message);
        }
        Err(error) => {
            observability::record_security_event("magika_quarantine");
            message.status = "quarantined".to_string();
            message.magika_decision = Some("quarantine".to_string());
            message.magika_summary = Some(format!("Magika validation failed: {error}"));
            message.security_score += 4.0;
            message.decision_trace.push(DecisionTraceEntry {
                stage: "magika".to_string(),
                outcome: "quarantine".to_string(),
                detail: message.magika_summary.clone().unwrap_or_default(),
            });
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, "quarantine", &message);
            info!(
                trace_id = %message.id,
                status = %message.status,
                "inbound message quarantined after Magika failure"
            );
            return Ok(message);
        }
    }

    let verdict = evaluate_inbound_policy(
        spool_dir,
        config,
        parse_peer_ip(&message.peer),
        &message.helo,
        &message.mail_from,
        &message.rcpt_to,
        &message.data,
    )
    .await?;
    apply_filter_verdict(&mut message, &verdict);

    match verdict.action {
        FilterAction::Accept => match deliver_inbound_message(config, &message).await {
            Ok(_) => {
                message.status = "sent".to_string();
                message.decision_trace.push(DecisionTraceEntry {
                    stage: "core-delivery".to_string(),
                    outcome: "sent".to_string(),
                    detail: "message delivered to the core LPE inbound-delivery API".to_string(),
                });
                move_message(spool_dir, &message, "incoming", "sent").await?;
                let _ = append_transport_audit(spool_dir, "sent", &message);
                update_reputation(spool_dir, config, &message, FilterAction::Accept).await?;
                train_bayespam(spool_dir, config, &message, BayesLabel::Ham).await?;
                observability::record_smtp_session("delivered");
            }
            Err(error) => {
                message.status = if config.fallback_to_hold_queue {
                    "held".to_string()
                } else {
                    "deferred".to_string()
                };
                message.relay_error = Some(error.to_string());
                message.decision_trace.push(DecisionTraceEntry {
                    stage: "core-delivery".to_string(),
                    outcome: message.status.clone(),
                    detail: error.to_string(),
                });
                let destination = if config.fallback_to_hold_queue {
                    "held"
                } else {
                    "deferred"
                };
                move_message(spool_dir, &message, "incoming", destination).await?;
                let _ = append_transport_audit(spool_dir, destination, &message);
                update_reputation(spool_dir, config, &message, FilterAction::Defer).await?;
                observability::record_security_event("inbound_delivery_deferred");
                observability::record_smtp_session("deferred");
            }
        },
        FilterAction::Quarantine => {
            observability::record_security_event("inbound_quarantine");
            message.status = "quarantined".to_string();
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, "quarantine", &message);
            update_reputation(spool_dir, config, &message, FilterAction::Quarantine).await?;
            train_bayespam(spool_dir, config, &message, BayesLabel::Spam).await?;
            observability::record_smtp_session("quarantined");
        }
        FilterAction::Reject => {
            observability::record_security_event("inbound_reject");
            message.status = "rejected".to_string();
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            persist_quarantine_metadata_or_warn(spool_dir, config, &message).await;
            let _ = append_transport_audit(spool_dir, "quarantine", &message);
            update_reputation(spool_dir, config, &message, FilterAction::Reject).await?;
            train_bayespam(spool_dir, config, &message, BayesLabel::Spam).await?;
            observability::record_smtp_session("rejected");
        }
        FilterAction::Defer => {
            observability::record_security_event("inbound_defer");
            message.status = "deferred".to_string();
            move_message(spool_dir, &message, "incoming", "deferred").await?;
            let _ = append_transport_audit(spool_dir, "deferred", &message);
            update_reputation(spool_dir, config, &message, FilterAction::Defer).await?;
            observability::record_smtp_session("deferred");
        }
    }

    info!(
        trace_id = %message.id,
        status = %message.status,
        peer = %message.peer,
        sender = %message.mail_from,
        recipient_count = message.rcpt_to.len(),
        "smtp message processed"
    );
    Ok(message)
}

fn classify_inbound_message<D: Detector>(
    validator: &Validator<D>,
    message_bytes: &[u8],
) -> Result<InboundMagikaOutcome> {
    let attachments = collect_mime_attachment_parts(message_bytes)?;
    for attachment in attachments {
        let outcome = validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::LpeCtInboundSmtp,
                declared_mime: attachment.declared_mime.clone(),
                filename: attachment.filename.clone(),
                expected_kind: ExpectedKind::Any,
            },
            &attachment.bytes,
        )?;
        match outcome.policy_decision {
            PolicyDecision::Accept => {}
            PolicyDecision::Reject => {
                return Ok(InboundMagikaOutcome::Reject(format!(
                    "attachment {:?} rejected: {}",
                    attachment.filename, outcome.reason
                )));
            }
            PolicyDecision::Quarantine | PolicyDecision::Restrict => {
                return Ok(InboundMagikaOutcome::Quarantine(format!(
                    "attachment {:?} quarantined: {}",
                    attachment.filename, outcome.reason
                )));
            }
        }
    }
    Ok(InboundMagikaOutcome::Accept)
}

async fn evaluate_antivirus_policy(
    config: &RuntimeConfig,
    direction: &str,
    message_bytes: &[u8],
) -> Result<AntivirusVerdict> {
    let mut decision_trace = Vec::new();
    if !config.antivirus_enabled {
        decision_trace.push(DecisionTraceEntry {
            stage: "virus-scan".to_string(),
            outcome: "disabled".to_string(),
            detail: "antivirus chain disabled by local policy".to_string(),
        });
        return Ok(AntivirusVerdict {
            action: FilterAction::Accept,
            reason: None,
            spam_score_delta: 0.0,
            security_score_delta: 0.0,
            decision_trace,
        });
    }

    if config.antivirus_provider_chain.is_empty() {
        let detail =
            "antivirus chain enabled but no providers are configured in LPE_CT_ANTIVIRUS_PROVIDER_CHAIN"
                .to_string();
        decision_trace.push(DecisionTraceEntry {
            stage: "virus-scan".to_string(),
            outcome: if config.antivirus_fail_closed {
                "quarantine"
            } else {
                "skipped"
            }
            .to_string(),
            detail: detail.clone(),
        });
        return Ok(AntivirusVerdict {
            action: if config.antivirus_fail_closed {
                FilterAction::Quarantine
            } else {
                FilterAction::Accept
            },
            reason: config.antivirus_fail_closed.then_some(detail),
            spam_score_delta: 0.0,
            security_score_delta: if config.antivirus_fail_closed {
                2.0
            } else {
                0.0
            },
            decision_trace,
        });
    }

    if config.antivirus_providers.is_empty() {
        let detail = format!(
            "antivirus chain references unsupported or incomplete providers: {}",
            config.antivirus_provider_chain.join(", ")
        );
        decision_trace.push(DecisionTraceEntry {
            stage: "virus-scan".to_string(),
            outcome: if config.antivirus_fail_closed {
                "quarantine"
            } else {
                "skipped"
            }
            .to_string(),
            detail: detail.clone(),
        });
        return Ok(AntivirusVerdict {
            action: if config.antivirus_fail_closed {
                FilterAction::Quarantine
            } else {
                FilterAction::Accept
            },
            reason: config.antivirus_fail_closed.then_some(detail),
            spam_score_delta: 0.0,
            security_score_delta: if config.antivirus_fail_closed {
                2.0
            } else {
                0.0
            },
            decision_trace,
        });
    }

    let target = prepare_antivirus_scan_target(direction, message_bytes)?;
    for provider in &config.antivirus_providers {
        match run_antivirus_provider(provider, &target).await {
            Ok(outcome) => {
                decision_trace.push(DecisionTraceEntry {
                    stage: "virus-scan".to_string(),
                    outcome: match outcome.decision {
                        AntivirusProviderDecision::Clean => "clean",
                        AntivirusProviderDecision::Suspicious => "suspicious",
                        AntivirusProviderDecision::Infected => "infected",
                    }
                    .to_string(),
                    detail: format!("{}: {}", outcome.provider_name, outcome.summary),
                });
                match outcome.decision {
                    AntivirusProviderDecision::Clean => {}
                    AntivirusProviderDecision::Suspicious => {
                        cleanup_antivirus_scan_target(&target);
                        return Ok(AntivirusVerdict {
                            action: FilterAction::Quarantine,
                            reason: Some(format!(
                                "antivirus provider {} flagged suspicious content",
                                outcome.provider_id
                            )),
                            spam_score_delta: 0.5,
                            security_score_delta: 4.0,
                            decision_trace,
                        });
                    }
                    AntivirusProviderDecision::Infected => {
                        cleanup_antivirus_scan_target(&target);
                        return Ok(AntivirusVerdict {
                            action: FilterAction::Quarantine,
                            reason: Some(format!(
                                "antivirus provider {} detected malware",
                                outcome.provider_id
                            )),
                            spam_score_delta: 1.0,
                            security_score_delta: 8.0,
                            decision_trace,
                        });
                    }
                }
            }
            Err(error) => {
                let detail = format!(
                    "{} execution failed for {} attachment artifact(s): {error}",
                    provider.display_name, target.attachment_count
                );
                decision_trace.push(DecisionTraceEntry {
                    stage: "virus-scan".to_string(),
                    outcome: if config.antivirus_fail_closed {
                        "quarantine"
                    } else {
                        "error"
                    }
                    .to_string(),
                    detail: detail.clone(),
                });
                if config.antivirus_fail_closed {
                    cleanup_antivirus_scan_target(&target);
                    return Ok(AntivirusVerdict {
                        action: FilterAction::Quarantine,
                        reason: Some(detail),
                        spam_score_delta: 0.0,
                        security_score_delta: 3.0,
                        decision_trace,
                    });
                }
            }
        }
    }

    cleanup_antivirus_scan_target(&target);
    Ok(AntivirusVerdict {
        action: FilterAction::Accept,
        reason: None,
        spam_score_delta: 0.0,
        security_score_delta: 0.0,
        decision_trace,
    })
}

fn prepare_antivirus_scan_target(
    direction: &str,
    message_bytes: &[u8],
) -> Result<AntivirusScanTarget> {
    let root = env::temp_dir().join(format!("lpe-ct-av-{}-{}", direction, uuid::Uuid::new_v4()));
    fs::create_dir_all(&root)
        .with_context(|| format!("unable to create antivirus scan target {}", root.display()))?;
    fs::write(root.join("message.eml"), message_bytes).with_context(|| {
        format!(
            "unable to write antivirus message artifact {}",
            root.display()
        )
    })?;

    let attachments = collect_mime_attachment_parts(message_bytes)?;
    for (index, attachment) in attachments.iter().enumerate() {
        let original_name = attachment.filename.as_deref().unwrap_or("attachment");
        let extension = attachment
            .filename
            .as_deref()
            .and_then(|filename| Path::new(filename).extension())
            .and_then(|value| value.to_str())
            .map(|value| format!(".{}", sanitize_attachment_component(value)))
            .unwrap_or_default();
        let file_name = Path::new(original_name)
            .file_stem()
            .and_then(|value| value.to_str())
            .map(sanitize_attachment_component)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("attachment-{}", index + 1));
        fs::write(
            root.join(format!("{:02}-{}{}", index + 1, file_name, extension)),
            &attachment.bytes,
        )
        .with_context(|| {
            format!(
                "unable to write antivirus attachment artifact {}",
                root.display()
            )
        })?;
    }

    Ok(AntivirusScanTarget {
        root,
        attachment_count: attachments.len(),
    })
}

fn sanitize_attachment_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn cleanup_antivirus_scan_target(target: &AntivirusScanTarget) {
    let _ = fs::remove_dir_all(&target.root);
}

async fn run_antivirus_provider(
    provider: &AntivirusProviderConfig,
    target: &AntivirusScanTarget,
) -> Result<AntivirusScanOutcome> {
    let mut command = Command::new(&provider.command);
    let target_path = target.root.to_string_lossy().to_string();
    let mut path_explicitly_bound = false;
    for arg in &provider.args {
        if arg.contains("{path}") {
            path_explicitly_bound = true;
        }
        command.arg(arg.replace("{path}", &target_path));
    }
    if !path_explicitly_bound {
        command.arg(&target.root);
    }
    let output = command.output().await.with_context(|| {
        format!(
            "unable to execute antivirus provider {}",
            provider.display_name
        )
    })?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    parse_antivirus_output(provider, &stdout, &stderr, output.status.code())
}

fn parse_antivirus_output(
    provider: &AntivirusProviderConfig,
    stdout: &str,
    stderr: &str,
    exit_code: Option<i32>,
) -> Result<AntivirusScanOutcome> {
    let combined = format!("{stdout}\n{stderr}");
    let normalized = combined.to_ascii_lowercase();
    let infected = marker_matches(&normalized, &provider.infected_markers)
        || takeri_summary_count(&normalized, "infected files:") > 0;
    let suspicious = marker_matches(&normalized, &provider.suspicious_markers)
        || takeri_summary_count(&normalized, "suspicious files:") > 0;
    let clean = marker_matches(&normalized, &provider.clean_markers);

    let decision = if infected {
        AntivirusProviderDecision::Infected
    } else if suspicious {
        AntivirusProviderDecision::Suspicious
    } else if clean || exit_code == Some(0) {
        AntivirusProviderDecision::Clean
    } else {
        anyhow::bail!(
            "provider {} returned exit code {:?} without a parsable verdict",
            provider.display_name,
            exit_code
        );
    };

    let summary = combined
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("provider produced no output")
        .to_string();

    Ok(AntivirusScanOutcome {
        provider_id: provider.id.clone(),
        provider_name: provider.display_name.clone(),
        decision,
        summary,
    })
}

fn marker_matches(output: &str, markers: &[String]) -> bool {
    markers
        .iter()
        .map(|marker| marker.trim().to_ascii_lowercase())
        .filter(|marker| !marker.is_empty())
        .any(|marker| output.contains(&marker))
}

fn takeri_summary_count(output: &str, prefix: &str) -> usize {
    output
        .lines()
        .find_map(|line| {
            let trimmed = line.trim();
            let normalized = trimmed.to_ascii_lowercase();
            normalized
                .strip_prefix(prefix)
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0)
}

async fn evaluate_inbound_policy(
    spool_dir: &Path,
    config: &RuntimeConfig,
    peer_ip: Option<IpAddr>,
    helo: &str,
    mail_from: &str,
    rcpt_to: &[String],
    message_bytes: &[u8],
) -> Result<FilterVerdict> {
    let mut spam_score = 0.0;
    let mut security_score = 0.0;
    let mut decision_trace = Vec::new();
    let mut dnsbl = DnsblOutcome::default();
    let mut auth_summary = AuthSummary::default();
    let mut auth_assessment = None;
    let mut defer_reasons = Vec::new();
    let mut reject_reasons = Vec::new();
    let mut quarantine_reasons = Vec::new();
    let reputation_score = if config.reputation_enabled {
        load_reputation_score(spool_dir, config, peer_ip, mail_from).await?
    } else {
        0
    };

    if config.quarantine_enabled && should_quarantine(message_bytes) {
        let reasons = vec!["message matched local quarantine policy".to_string()];
        decision_trace.push(DecisionTraceEntry {
            stage: "manual-quarantine".to_string(),
            outcome: "quarantine".to_string(),
            detail: "message matched the explicit quarantine marker policy".to_string(),
        });
        return Ok(FilterVerdict {
            action: FilterAction::Quarantine,
            reason: Some(reasons.join("; ")),
            spam_score: config.spam_quarantine_threshold.max(1.0),
            security_score: 1.0,
            reputation_score,
            dnsbl_hits: dnsbl.hits,
            auth_summary,
            decision_trace,
        });
    }

    decision_trace.push(DecisionTraceEntry {
        stage: "pipeline".to_string(),
        outcome: "start".to_string(),
        detail: "running inbound edge pipeline: rbl/dns, bayespam, antivirus chain, final scoring"
            .to_string(),
    });

    if let Some(ip) = peer_ip {
        if config.greylisting_enabled {
            match evaluate_greylisting(spool_dir, config, ip, mail_from, rcpt_to).await? {
                Some(reason) => {
                    decision_trace.push(DecisionTraceEntry {
                        stage: "greylisting".to_string(),
                        outcome: "defer".to_string(),
                        detail: reason.clone(),
                    });
                    spam_score += 1.5;
                    return Ok(FilterVerdict {
                        action: FilterAction::Defer,
                        reason: Some(reason),
                        spam_score,
                        security_score,
                        reputation_score,
                        dnsbl_hits: dnsbl.hits,
                        auth_summary,
                        decision_trace,
                    });
                }
                None => {
                    decision_trace.push(DecisionTraceEntry {
                        stage: "greylisting".to_string(),
                        outcome: "pass".to_string(),
                        detail: "triplet already aged through greylisting".to_string(),
                    });
                }
            }
        }

        if config.dnsbl_enabled {
            dnsbl = query_dnsbl(ip, &config.dnsbl_zones).await;
            if !dnsbl.hits.is_empty() {
                spam_score += 4.0 + dnsbl.hits.len() as f32;
                security_score += 2.0;
                decision_trace.push(DecisionTraceEntry {
                    stage: "rbl-dns-check".to_string(),
                    outcome: "listed".to_string(),
                    detail: format!("source IP listed on {}", dnsbl.hits.join(", ")),
                });
            } else {
                decision_trace.push(DecisionTraceEntry {
                    stage: "rbl-dns-check".to_string(),
                    outcome: "clear".to_string(),
                    detail: "source IP not listed on configured DNSBL zones".to_string(),
                });
            }
            if !dnsbl.tempfail_zones.is_empty() {
                security_score += 0.5;
                decision_trace.push(DecisionTraceEntry {
                    stage: "rbl-dns-check".to_string(),
                    outcome: "temperror".to_string(),
                    detail: format!(
                        "temporary DNS failure while querying {}",
                        dnsbl.tempfail_zones.join(", ")
                    ),
                });
            }
        }

        match authenticate_message(ip, helo, mail_from, message_bytes).await {
            Ok((summary, auth_trace, assessment)) => {
                auth_summary = summary;
                auth_assessment = Some(assessment.clone());
                decision_trace.extend(auth_trace);
                apply_authentication_scores(
                    &assessment,
                    &mut spam_score,
                    &mut security_score,
                    &mut decision_trace,
                );
            }
            Err(error) => {
                security_score += 1.0;
                decision_trace.push(DecisionTraceEntry {
                    stage: "authentication".to_string(),
                    outcome: "temperror".to_string(),
                    detail: format!(
                        "authentication checks failed open with resolver error: {error}"
                    ),
                });
            }
        }
    } else {
        decision_trace.push(DecisionTraceEntry {
            stage: "authentication".to_string(),
            outcome: "skipped".to_string(),
            detail: "source peer IP could not be parsed for SPF, DKIM, and DMARC evaluation"
                .to_string(),
        });
    }

    let subject = parse_rfc822_header_value(message_bytes, "subject").unwrap_or_default();
    let visible_text = extract_visible_text(message_bytes)?;
    match score_bayespam(spool_dir, config, &subject, &visible_text, mail_from, helo).await? {
        Some(outcome) => {
            spam_score += outcome.contribution;
            decision_trace.push(DecisionTraceEntry {
                stage: "bayespam".to_string(),
                outcome: if outcome.probability >= 0.90 {
                    "spam"
                } else if outcome.probability >= 0.70 {
                    "suspect"
                } else {
                    "ham"
                }
                .to_string(),
                detail: format!(
                    "bayespam probability {:.3} using {} learned tokens (contribution={:.2})",
                    outcome.probability, outcome.matched_tokens, outcome.contribution
                ),
            });
        }
        None if config.bayespam_enabled => {
            decision_trace.push(DecisionTraceEntry {
                stage: "bayespam".to_string(),
                outcome: "skipped".to_string(),
                detail: "bayespam corpus is not trained enough for scoring yet".to_string(),
            });
        }
        None => {
            decision_trace.push(DecisionTraceEntry {
                stage: "bayespam".to_string(),
                outcome: "disabled".to_string(),
                detail: "bayespam disabled by local policy".to_string(),
            });
        }
    }
    let antivirus_verdict = evaluate_antivirus_policy(config, "inbound", message_bytes).await?;
    spam_score += antivirus_verdict.spam_score_delta;
    security_score += antivirus_verdict.security_score_delta;
    if antivirus_verdict.action == FilterAction::Quarantine {
        if let Some(reason) = antivirus_verdict.reason.clone() {
            quarantine_reasons.push(reason);
        }
    }
    decision_trace.extend(antivirus_verdict.decision_trace);

    if reputation_score < 0 {
        spam_score += (-reputation_score) as f32 * 0.35;
        security_score += (-reputation_score) as f32 * 0.10;
        decision_trace.push(DecisionTraceEntry {
            stage: "reputation".to_string(),
            outcome: "negative".to_string(),
            detail: format!("historical reputation score is {}", reputation_score),
        });
    } else {
        decision_trace.push(DecisionTraceEntry {
            stage: "reputation".to_string(),
            outcome: "neutral".to_string(),
            detail: format!("historical reputation score is {}", reputation_score),
        });
    }

    if config.reputation_enabled && reputation_score <= config.reputation_reject_threshold {
        reject_reasons.push(format!(
            "reputation score {} reached reject threshold {}",
            reputation_score, config.reputation_reject_threshold
        ));
        decision_trace.push(DecisionTraceEntry {
            stage: "reputation".to_string(),
            outcome: "reject".to_string(),
            detail: format!(
                "historical reputation score {} reached reject threshold {}",
                reputation_score, config.reputation_reject_threshold
            ),
        });
    } else if config.reputation_enabled
        && reputation_score <= config.reputation_quarantine_threshold
    {
        quarantine_reasons.push(format!(
            "reputation score {} reached quarantine threshold {}",
            reputation_score, config.reputation_quarantine_threshold
        ));
        decision_trace.push(DecisionTraceEntry {
            stage: "reputation".to_string(),
            outcome: "quarantine".to_string(),
            detail: format!(
                "historical reputation score {} reached quarantine threshold {}",
                reputation_score, config.reputation_quarantine_threshold
            ),
        });
    }

    if config.defer_on_auth_tempfail
        && auth_assessment
            .as_ref()
            .is_some_and(AuthenticationAssessment::has_temporary_failure)
    {
        defer_reasons.push("authentication dependency temporarily failed".to_string());
    }
    if config.require_dmarc_enforcement
        && auth_assessment
            .as_ref()
            .is_some_and(|assessment| assessment.dmarc == DmarcDisposition::Reject)
    {
        reject_reasons.push("DMARC policy requested reject".to_string());
    }
    if config.require_dmarc_enforcement
        && auth_assessment
            .as_ref()
            .is_some_and(|assessment| assessment.dmarc == DmarcDisposition::Quarantine)
    {
        quarantine_reasons.push("DMARC policy requested quarantine".to_string());
    }
    if config.require_spf
        && auth_assessment.as_ref().is_some_and(|assessment| {
            assessment.spf == SpfDisposition::Fail && !assessment.dkim_aligned
        })
    {
        reject_reasons.push("SPF failed and no aligned DKIM signature passed".to_string());
    }
    if config.require_dkim_alignment
        && auth_assessment
            .as_ref()
            .is_some_and(|assessment| !assessment.dkim_aligned)
    {
        quarantine_reasons.push("aligned DKIM verification did not pass".to_string());
    }
    if spam_score >= config.spam_reject_threshold {
        reject_reasons.push(format!(
            "spam score {:.1} reached reject threshold {:.1}",
            spam_score, config.spam_reject_threshold
        ));
    } else if spam_score >= config.spam_quarantine_threshold {
        quarantine_reasons.push(format!(
            "spam score {:.1} reached quarantine threshold {:.1}",
            spam_score, config.spam_quarantine_threshold
        ));
    }

    decision_trace.push(DecisionTraceEntry {
        stage: "final-score".to_string(),
        outcome: "calculated".to_string(),
        detail: format!(
            "spam_score={spam_score:.1} security_score={security_score:.1} reputation_score={reputation_score}"
        ),
    });

    for reason in &defer_reasons {
        decision_trace.push(DecisionTraceEntry {
            stage: "policy-trigger".to_string(),
            outcome: "defer".to_string(),
            detail: reason.clone(),
        });
    }
    for reason in &reject_reasons {
        decision_trace.push(DecisionTraceEntry {
            stage: "policy-trigger".to_string(),
            outcome: "reject".to_string(),
            detail: reason.clone(),
        });
    }
    for reason in &quarantine_reasons {
        decision_trace.push(DecisionTraceEntry {
            stage: "policy-trigger".to_string(),
            outcome: "quarantine".to_string(),
            detail: reason.clone(),
        });
    }

    let (action, reasons) = if !defer_reasons.is_empty() {
        (FilterAction::Defer, defer_reasons)
    } else if !reject_reasons.is_empty() {
        (FilterAction::Reject, reject_reasons)
    } else if !quarantine_reasons.is_empty() {
        (FilterAction::Quarantine, quarantine_reasons)
    } else {
        (FilterAction::Accept, Vec::new())
    };

    let reason = if reasons.is_empty() {
        None
    } else {
        Some(reasons.join("; "))
    };

    decision_trace.push(DecisionTraceEntry {
        stage: "final-policy".to_string(),
        outcome: match action {
            FilterAction::Accept => "accept",
            FilterAction::Quarantine => "quarantine",
            FilterAction::Reject => "reject",
            FilterAction::Defer => "defer",
        }
        .to_string(),
        detail: reason
            .clone()
            .unwrap_or_else(|| {
                format!(
                    "message passed SMTP perimeter policy (spam_score={spam_score:.1}, security_score={security_score:.1})"
                )
            }),
    });

    Ok(FilterVerdict {
        action,
        reason,
        spam_score,
        security_score,
        reputation_score,
        dnsbl_hits: dnsbl.hits,
        auth_summary,
        decision_trace,
    })
}

fn apply_filter_verdict(message: &mut QueuedMessage, verdict: &FilterVerdict) {
    message.spam_score = verdict.spam_score;
    message.security_score = verdict.security_score;
    message.reputation_score = verdict.reputation_score;
    message.dnsbl_hits = verdict.dnsbl_hits.clone();
    message.auth_summary = verdict.auth_summary.clone();
    message
        .decision_trace
        .extend(verdict.decision_trace.clone());
    if let Some(reason) = &verdict.reason {
        message.relay_error = Some(reason.clone());
    }
}

fn apply_authentication_scores(
    assessment: &AuthenticationAssessment,
    spam_score: &mut f32,
    security_score: &mut f32,
    decision_trace: &mut Vec<DecisionTraceEntry>,
) {
    match assessment.spf {
        SpfDisposition::SoftFail => {
            *spam_score += 1.5;
            decision_trace.push(DecisionTraceEntry {
                stage: "spf".to_string(),
                outcome: "softfail".to_string(),
                detail: "SPF softfail increases spam score without forcing a reject".to_string(),
            });
        }
        SpfDisposition::Fail => {
            *security_score += 2.5;
        }
        SpfDisposition::PermError => {
            *security_score += 1.5;
            decision_trace.push(DecisionTraceEntry {
                stage: "spf".to_string(),
                outcome: "permerror".to_string(),
                detail: "SPF record is malformed or exceeded processing limits".to_string(),
            });
        }
        SpfDisposition::TempError => {
            *security_score += 1.0;
        }
        _ => {}
    }

    match assessment.dkim {
        DkimDisposition::Fail => {
            *spam_score += 1.0;
            *security_score += 1.0;
        }
        DkimDisposition::PermFail => {
            *spam_score += 1.5;
            *security_score += 1.5;
            decision_trace.push(DecisionTraceEntry {
                stage: "dkim".to_string(),
                outcome: "permfail".to_string(),
                detail: "DKIM signature or key policy is structurally invalid".to_string(),
            });
        }
        DkimDisposition::TempFail => {
            *security_score += 1.0;
        }
        _ => {}
    }

    match assessment.dmarc {
        DmarcDisposition::Quarantine => {
            *spam_score += 3.0;
            *security_score += 1.0;
        }
        DmarcDisposition::Reject => {
            *security_score += 4.0;
        }
        DmarcDisposition::TempFail => {
            *security_score += 2.0;
        }
        _ => {}
    }

    if !assessment.spf_aligned {
        decision_trace.push(DecisionTraceEntry {
            stage: "spf-alignment".to_string(),
            outcome: "misaligned".to_string(),
            detail: format!(
                "RFC 5322 From domain {} is not aligned with SPF domain {}",
                assessment.from_domain, assessment.spf_domain
            ),
        });
    }
    if !assessment.dkim_aligned {
        decision_trace.push(DecisionTraceEntry {
            stage: "dkim-alignment".to_string(),
            outcome: "misaligned".to_string(),
            detail: format!(
                "no aligned DKIM signature passed for RFC 5322 From domain {}",
                assessment.from_domain
            ),
        });
    }
}

fn spf_disposition(result: &SpfResult) -> SpfDisposition {
    match result {
        SpfResult::Pass => SpfDisposition::Pass,
        SpfResult::Fail { .. } => SpfDisposition::Fail,
        SpfResult::SoftFail => SpfDisposition::SoftFail,
        SpfResult::Neutral => SpfDisposition::Neutral,
        SpfResult::None => SpfDisposition::None,
        SpfResult::TempError => SpfDisposition::TempError,
        SpfResult::PermError => SpfDisposition::PermError,
    }
}

fn dkim_disposition(results: &[DkimResult]) -> DkimDisposition {
    if results
        .iter()
        .any(|result| matches!(result, DkimResult::Pass { .. }))
    {
        DkimDisposition::Pass
    } else if results
        .iter()
        .any(|result| matches!(result, DkimResult::TempFail { .. }))
    {
        DkimDisposition::TempFail
    } else if results
        .iter()
        .any(|result| matches!(result, DkimResult::PermFail { .. }))
    {
        DkimDisposition::PermFail
    } else if results
        .iter()
        .any(|result| matches!(result, DkimResult::Fail { .. }))
    {
        DkimDisposition::Fail
    } else {
        DkimDisposition::None
    }
}

fn summarize_spf(result: &SpfResult) -> String {
    match result {
        SpfResult::Pass => "pass".to_string(),
        SpfResult::Fail { explanation } => match explanation {
            Some(explanation) if !explanation.trim().is_empty() => {
                format!("fail ({})", explanation.trim())
            }
            _ => "fail".to_string(),
        },
        SpfResult::SoftFail => "softfail".to_string(),
        SpfResult::Neutral => "neutral".to_string(),
        SpfResult::None => "none".to_string(),
        SpfResult::TempError => "temperror".to_string(),
        SpfResult::PermError => "permerror".to_string(),
    }
}

fn summarize_dkim(results: &[DkimResult], aligned: bool) -> String {
    match dkim_disposition(results) {
        DkimDisposition::Pass if aligned => "pass (aligned)".to_string(),
        DkimDisposition::Pass => "pass (unaligned)".to_string(),
        DkimDisposition::Fail => "fail".to_string(),
        DkimDisposition::TempFail => "temperror".to_string(),
        DkimDisposition::PermFail => "permerror".to_string(),
        DkimDisposition::None => "none".to_string(),
    }
}

fn summarize_dmarc(result: DmarcDisposition) -> String {
    match result {
        DmarcDisposition::Pass => "pass".to_string(),
        DmarcDisposition::Quarantine => "quarantine".to_string(),
        DmarcDisposition::Reject => "reject".to_string(),
        DmarcDisposition::None => "none".to_string(),
        DmarcDisposition::TempFail => "temperror".to_string(),
    }
}

async fn authenticate_message(
    client_ip: IpAddr,
    helo: &str,
    mail_from: &str,
    message_bytes: &[u8],
) -> Result<(
    AuthSummary,
    Vec<DecisionTraceEntry>,
    AuthenticationAssessment,
)> {
    let authenticator = EmailAuthenticator::new(SystemDnsResolver::new()?, "lpe-ct.local");
    let result = authenticator
        .authenticate(message_bytes, client_ip, helo, mail_from)
        .await
        .map_err(|error| anyhow!("authentication evaluation failed: {error}"))?;

    let spf = summarize_spf(&result.spf);
    let dkim = summarize_dkim(&result.dkim, result.dmarc.dkim_aligned);
    let dmarc = summarize_dmarc(result.dmarc.disposition);
    let assessment = AuthenticationAssessment {
        spf: spf_disposition(&result.spf),
        dkim: dkim_disposition(&result.dkim),
        dkim_aligned: result.dmarc.dkim_aligned,
        spf_aligned: result.dmarc.spf_aligned,
        dmarc: result.dmarc.disposition,
        from_domain: result.from_domain.clone(),
        spf_domain: result.spf_domain.clone(),
    };
    let mut trace = vec![
        DecisionTraceEntry {
            stage: "spf".to_string(),
            outcome: spf.clone(),
            detail: format!(
                "SPF evaluation for envelope sender {} from {} using domain {}",
                mail_from, client_ip, result.spf_domain
            ),
        },
        DecisionTraceEntry {
            stage: "dkim".to_string(),
            outcome: dkim.clone(),
            detail: format!(
                "DKIM verification executed on the RFC 5322 message (aligned={})",
                result.dmarc.dkim_aligned
            ),
        },
        DecisionTraceEntry {
            stage: "dmarc".to_string(),
            outcome: dmarc.clone(),
            detail: format!(
                "DMARC evaluation executed for RFC 5322 From domain {} (spf_aligned={}, dkim_aligned={})",
                result.from_domain, result.dmarc.spf_aligned, result.dmarc.dkim_aligned
            ),
        },
    ];

    if assessment.has_temporary_failure() {
        trace.push(DecisionTraceEntry {
            stage: "authentication".to_string(),
            outcome: "temperror".to_string(),
            detail: "one of SPF, DKIM, or DMARC encountered a temporary failure".to_string(),
        });
    }

    Ok((AuthSummary { spf, dkim, dmarc }, trace, assessment))
}

async fn query_dnsbl(ip: IpAddr, zones: &[String]) -> DnsblOutcome {
    let resolver = match SystemDnsResolver::new() {
        Ok(resolver) => resolver,
        Err(_) => {
            return DnsblOutcome {
                hits: Vec::new(),
                tempfail_zones: zones.to_vec(),
            };
        }
    };
    let mut outcome = DnsblOutcome::default();
    for zone in zones {
        let query = dnsbl_query_name(ip, zone);
        match resolver.query_exists(&query).await {
            Ok(true) => outcome.hits.push(zone.clone()),
            Ok(false) | Err(DnsError::NxDomain) | Err(DnsError::NoRecords) => {}
            Err(DnsError::TempFail) => outcome.tempfail_zones.push(zone.clone()),
        }
    }
    outcome
}

fn dnsbl_query_name(ip: IpAddr, zone: &str) -> String {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            format!(
                "{}.{}.{}.{}.{}",
                octets[3], octets[2], octets[1], octets[0], zone
            )
        }
        IpAddr::V6(ip) => {
            let hex = ip
                .octets()
                .iter()
                .flat_map(|byte| [byte >> 4, byte & 0x0f])
                .map(|nibble| format!("{nibble:x}"))
                .collect::<Vec<_>>();
            format!(
                "{}.{}",
                hex.into_iter().rev().collect::<Vec<_>>().join("."),
                zone
            )
        }
    }
}

fn parse_peer_ip(peer: &str) -> Option<IpAddr> {
    if let Ok(addr) = peer.parse::<SocketAddr>() {
        return Some(addr.ip());
    }
    peer.parse::<IpAddr>().ok()
}

async fn evaluate_greylisting(
    spool_dir: &Path,
    config: &RuntimeConfig,
    ip: IpAddr,
    mail_from: &str,
    rcpt_to: &[String],
) -> Result<Option<String>> {
    let rcpt = rcpt_to.first().map(String::as_str).unwrap_or_default();
    let key = stable_key_id(&(
        ip,
        mail_from.to_ascii_lowercase(),
        rcpt.to_ascii_lowercase(),
    ));
    let now = unix_now();
    let mut entry = if let Some(pool) = ensure_local_db_schema(config).await? {
        let row = sqlx::query("SELECT state FROM greylist_entries WHERE entry_key = $1")
            .bind(&key)
            .fetch_optional(pool)
            .await?;
        row.map(|row| row.try_get::<Json<GreylistEntry>, _>("state"))
            .transpose()?
            .map(|value| value.0)
            .unwrap_or_else(|| GreylistEntry {
                first_seen_unix: now,
                release_after_unix: now + GREYLIST_DELAY_SECONDS,
                pass_count: 0,
            })
    } else {
        let path = spool_dir.join("greylist").join(format!("{key}.json"));
        if path.exists() {
            serde_json::from_str::<GreylistEntry>(&fs::read_to_string(&path)?)?
        } else {
            GreylistEntry {
                first_seen_unix: now,
                release_after_unix: now + GREYLIST_DELAY_SECONDS,
                pass_count: 0,
            }
        }
    };

    if now < entry.release_after_unix {
        if let Some(pool) = ensure_local_db_schema(config).await? {
            sqlx::query(
                r#"
                INSERT INTO greylist_entries (entry_key, state, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (entry_key) DO UPDATE SET
                    state = EXCLUDED.state,
                    updated_at = NOW()
                "#,
            )
            .bind(&key)
            .bind(Json(&entry))
            .execute(pool)
            .await?;
        } else {
            let path = spool_dir.join("greylist").join(format!("{key}.json"));
            if !path.exists() {
                fs::write(&path, serde_json::to_string_pretty(&entry)?)?;
            }
        }
        return Ok(Some(format!(
            "greylisted triplet {} for {} seconds",
            key, GREYLIST_DELAY_SECONDS
        )));
    }

    entry.pass_count += 1;
    if let Some(pool) = ensure_local_db_schema(config).await? {
        sqlx::query(
            r#"
            INSERT INTO greylist_entries (entry_key, state, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (entry_key) DO UPDATE SET
                state = EXCLUDED.state,
                updated_at = NOW()
            "#,
        )
        .bind(&key)
        .bind(Json(&entry))
        .execute(pool)
        .await?;
    } else {
        let path = spool_dir.join("greylist").join(format!("{key}.json"));
        fs::write(&path, serde_json::to_string_pretty(&entry)?)?;
    }
    Ok(None)
}

async fn load_reputation_score(
    spool_dir: &Path,
    config: &RuntimeConfig,
    peer_ip: Option<IpAddr>,
    mail_from: &str,
) -> Result<i32> {
    let key = reputation_key(peer_ip, mail_from);
    let entry = if let Some(pool) = ensure_local_db_schema(config).await? {
        let row = sqlx::query("SELECT state FROM reputation_entries WHERE entry_key = $1")
            .bind(&key)
            .fetch_optional(pool)
            .await?;
        row.map(|row| row.try_get::<Json<ReputationEntry>, _>("state"))
            .transpose()?
            .map(|value| value.0)
            .unwrap_or_default()
    } else {
        let store = load_reputation_store(spool_dir)?;
        store.entries.get(&key).cloned().unwrap_or_default()
    };
    Ok(entry.accepted as i32
        - entry.deferred as i32
        - (entry.quarantined as i32 * 2)
        - (entry.rejected as i32 * 3))
}

async fn update_reputation(
    spool_dir: &Path,
    config: &RuntimeConfig,
    message: &QueuedMessage,
    action: FilterAction,
) -> Result<()> {
    let key = reputation_key(parse_peer_ip(&message.peer), &message.mail_from);
    let mut entry = if let Some(pool) = ensure_local_db_schema(config).await? {
        let row = sqlx::query("SELECT state FROM reputation_entries WHERE entry_key = $1")
            .bind(&key)
            .fetch_optional(pool)
            .await?;
        row.map(|row| row.try_get::<Json<ReputationEntry>, _>("state"))
            .transpose()?
            .map(|value| value.0)
            .unwrap_or_default()
    } else {
        let store = load_reputation_store(spool_dir)?;
        store.entries.get(&key).cloned().unwrap_or_default()
    };
    match action {
        FilterAction::Accept => entry.accepted += 1,
        FilterAction::Quarantine => entry.quarantined += 1,
        FilterAction::Reject => entry.rejected += 1,
        FilterAction::Defer => entry.deferred += 1,
    }
    if let Some(pool) = ensure_local_db_schema(config).await? {
        sqlx::query(
            r#"
            INSERT INTO reputation_entries (entry_key, state, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (entry_key) DO UPDATE SET
                state = EXCLUDED.state,
                updated_at = NOW()
            "#,
        )
        .bind(&key)
        .bind(Json(&entry))
        .execute(pool)
        .await?;
        Ok(())
    } else {
        let mut store = load_reputation_store(spool_dir)?;
        store.entries.insert(key, entry);
        save_reputation_store(spool_dir, &store)
    }
}

fn reputation_key(peer_ip: Option<IpAddr>, mail_from: &str) -> String {
    format!(
        "{}|{}",
        peer_ip
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        sender_domain(mail_from)
    )
}

fn sender_domain(mail_from: &str) -> String {
    mail_from
        .split('@')
        .nth(1)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn load_reputation_store(spool_dir: &Path) -> Result<ReputationStore> {
    let path = spool_dir.join("policy").join("reputation.json");
    if !path.exists() {
        return Ok(ReputationStore::default());
    }
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

fn save_reputation_store(spool_dir: &Path, store: &ReputationStore) -> Result<()> {
    let path = spool_dir.join("policy").join("reputation.json");
    fs::write(path, serde_json::to_string_pretty(store)?)?;
    Ok(())
}

async fn load_bayespam_corpus(spool_dir: &Path, config: &RuntimeConfig) -> Result<BayesCorpus> {
    if let Some(pool) = ensure_local_db_schema(config).await? {
        let row = sqlx::query("SELECT corpus FROM bayespam_corpora WHERE corpus_key = $1")
            .bind("default")
            .fetch_optional(pool)
            .await?;
        return Ok(row
            .map(|row| row.try_get::<Json<BayesCorpus>, _>("corpus"))
            .transpose()?
            .map(|value| value.0)
            .unwrap_or_default());
    }

    let path = spool_dir.join("policy").join("bayespam.json");
    if !path.exists() {
        return Ok(BayesCorpus::default());
    }
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

async fn save_bayespam_corpus(
    spool_dir: &Path,
    config: &RuntimeConfig,
    corpus: &BayesCorpus,
) -> Result<()> {
    if let Some(pool) = ensure_local_db_schema(config).await? {
        sqlx::query(
            r#"
            INSERT INTO bayespam_corpora (corpus_key, corpus, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (corpus_key) DO UPDATE SET
                corpus = EXCLUDED.corpus,
                updated_at = NOW()
            "#,
        )
        .bind("default")
        .bind(Json(corpus))
        .execute(pool)
        .await?;
        return Ok(());
    }

    let path = spool_dir.join("policy").join("bayespam.json");
    fs::write(path, serde_json::to_string_pretty(corpus)?)?;
    Ok(())
}

fn tokenize_for_bayespam(
    subject: &str,
    visible_text: &str,
    mail_from: &str,
    helo: &str,
    min_token_length: usize,
    max_tokens: usize,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();
    for token in [subject, visible_text, mail_from, helo]
        .into_iter()
        .flat_map(|value| {
            value
                .split(|ch: char| !ch.is_ascii_alphanumeric())
                .map(str::trim)
                .filter(|token| token.len() >= min_token_length)
                .map(|token| token.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
    {
        if seen.insert(token.clone()) {
            tokens.push(token);
            if tokens.len() >= max_tokens {
                break;
            }
        }
    }
    tokens
}

fn bayespam_token_probability(corpus: &BayesCorpus, token: &str) -> Option<f64> {
    if corpus.ham_messages == 0 || corpus.spam_messages == 0 {
        return None;
    }
    let spam = (*corpus.spam_tokens.get(token).unwrap_or(&0) as f64 + 1.0)
        / (corpus.spam_messages as f64 + 2.0);
    let ham = (*corpus.ham_tokens.get(token).unwrap_or(&0) as f64 + 1.0)
        / (corpus.ham_messages as f64 + 2.0);
    let probability = spam / (spam + ham);
    Some(probability.clamp(0.01, 0.99))
}

fn score_bayespam_tokens(
    corpus: &BayesCorpus,
    tokens: &[String],
    score_weight: f32,
) -> Option<BayesOutcome> {
    if corpus.ham_messages == 0 || corpus.spam_messages == 0 {
        return None;
    }

    let mut log_spam = 0.0f64;
    let mut log_ham = 0.0f64;
    let mut matched = 0usize;
    for token in tokens {
        let Some(probability) = bayespam_token_probability(corpus, token) else {
            continue;
        };
        log_spam += probability.ln();
        log_ham += (1.0 - probability).ln();
        matched += 1;
    }

    if matched == 0 {
        return None;
    }

    let probability = 1.0 / (1.0 + (log_ham - log_spam).exp());
    let contribution = ((probability as f32 - 0.5).max(0.0) * 2.0) * score_weight.max(0.0);
    Some(BayesOutcome {
        probability: probability as f32,
        matched_tokens: matched,
        contribution,
    })
}

async fn score_bayespam(
    spool_dir: &Path,
    config: &RuntimeConfig,
    subject: &str,
    visible_text: &str,
    mail_from: &str,
    helo: &str,
) -> Result<Option<BayesOutcome>> {
    if !config.bayespam_enabled {
        return Ok(None);
    }
    let corpus = load_bayespam_corpus(spool_dir, config).await?;
    let tokens = tokenize_for_bayespam(
        subject,
        visible_text,
        mail_from,
        helo,
        config.bayespam_min_token_length.max(2) as usize,
        config.bayespam_max_tokens.max(16) as usize,
    );
    Ok(score_bayespam_tokens(
        &corpus,
        &tokens,
        config.bayespam_score_weight,
    ))
}

async fn train_bayespam(
    spool_dir: &Path,
    config: &RuntimeConfig,
    message: &QueuedMessage,
    label: BayesLabel,
) -> Result<()> {
    if !config.bayespam_enabled || !config.bayespam_auto_learn {
        return Ok(());
    }

    let subject = parse_rfc822_header_value(&message.data, "subject").unwrap_or_default();
    let visible_text = extract_visible_text(&message.data)?;
    let tokens = tokenize_for_bayespam(
        &subject,
        &visible_text,
        &message.mail_from,
        &message.helo,
        config.bayespam_min_token_length.max(2) as usize,
        config.bayespam_max_tokens.max(16) as usize,
    );
    if tokens.is_empty() {
        return Ok(());
    }

    let mut corpus = load_bayespam_corpus(spool_dir, config).await?;
    match label {
        BayesLabel::Ham => {
            corpus.ham_messages = corpus.ham_messages.saturating_add(1);
            for token in tokens {
                let entry = corpus.ham_tokens.entry(token).or_insert(0);
                *entry = entry.saturating_add(1);
            }
        }
        BayesLabel::Spam => {
            corpus.spam_messages = corpus.spam_messages.saturating_add(1);
            for token in tokens {
                let entry = corpus.spam_tokens.entry(token).or_insert(0);
                *entry = entry.saturating_add(1);
            }
        }
    }
    save_bayespam_corpus(spool_dir, config, &corpus).await
}

fn stable_key_id<T: Hash>(value: &T) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[derive(Clone)]
struct SystemDnsResolver {
    resolver: TokioResolver,
}

impl SystemDnsResolver {
    fn new() -> Result<Self> {
        let resolver = TokioResolver::builder_tokio()
            .context("unable to create DNS resolver builder from system configuration")?
            .build();
        Ok(Self { resolver })
    }
}

impl DnsResolver for SystemDnsResolver {
    async fn query_txt(&self, name: &str) -> Result<Vec<String>, DnsError> {
        let lookup = self
            .resolver
            .lookup(name, RecordType::TXT)
            .await
            .map_err(map_dns_error)?;
        Ok(lookup.iter().map(|record| record.to_string()).collect())
    }

    async fn query_a(&self, name: &str) -> Result<Vec<Ipv4Addr>, DnsError> {
        let lookup = self
            .resolver
            .ipv4_lookup(name)
            .await
            .map_err(map_dns_error)?;
        Ok(lookup.iter().map(|record| record.0).collect())
    }

    async fn query_aaaa(&self, name: &str) -> Result<Vec<Ipv6Addr>, DnsError> {
        let lookup = self
            .resolver
            .ipv6_lookup(name)
            .await
            .map_err(map_dns_error)?;
        Ok(lookup.iter().map(|record| record.0).collect())
    }

    async fn query_mx(&self, name: &str) -> Result<Vec<MxRecord>, DnsError> {
        let lookup = self.resolver.mx_lookup(name).await.map_err(map_dns_error)?;
        Ok(lookup
            .iter()
            .map(|record| MxRecord {
                preference: record.preference(),
                exchange: record.exchange().to_utf8(),
            })
            .collect())
    }

    async fn query_ptr(&self, ip: &IpAddr) -> Result<Vec<String>, DnsError> {
        let lookup = self
            .resolver
            .reverse_lookup(*ip)
            .await
            .map_err(map_dns_error)?;
        Ok(lookup.iter().map(|name| name.to_utf8()).collect())
    }

    async fn query_exists(&self, name: &str) -> Result<bool, DnsError> {
        let a = self.resolver.lookup_ip(name).await.map_err(map_dns_error)?;
        Ok(a.iter().next().is_some())
    }
}

fn map_dns_error(error: hickory_resolver::ResolveError) -> DnsError {
    let message = error.to_string().to_ascii_lowercase();
    if message.contains("nxdomain") || message.contains("no such domain") {
        DnsError::NxDomain
    } else if message.contains("no records") || message.contains("no data") {
        DnsError::NoRecords
    } else {
        DnsError::TempFail
    }
}

async fn deliver_inbound_message(
    config: &RuntimeConfig,
    message: &QueuedMessage,
) -> Result<InboundDeliveryResponse> {
    let endpoint = format!(
        "{}{}",
        config.core_delivery_base_url.trim_end_matches('/'),
        INBOUND_DELIVERY_PATH
    );
    let subject = parse_rfc822_header_value(&message.data, "subject").unwrap_or_default();
    let internet_message_id = parse_rfc822_header_value(&message.data, "message-id");
    let body_text = extract_visible_text(&message.data)?;
    let request = InboundDeliveryRequest {
        trace_id: message.id.clone(),
        peer: message.peer.clone(),
        helo: message.helo.clone(),
        mail_from: message.mail_from.clone(),
        rcpt_to: message.rcpt_to.clone(),
        subject,
        body_text,
        internet_message_id,
        raw_message: message.data.clone(),
    };

    let client = reqwest::Client::builder().build()?;
    let integration_secret = integration_shared_secret()?;
    let signed = SignedIntegrationHeaders::sign(
        &integration_secret,
        "POST",
        INBOUND_DELIVERY_PATH,
        &request,
    )
    .map_err(|error| anyhow!(error.to_string()))?;
    let response = client
        .post(endpoint)
        .header(INTEGRATION_KEY_HEADER, signed.integration_key)
        .header(INTEGRATION_TIMESTAMP_HEADER, signed.timestamp)
        .header(INTEGRATION_NONCE_HEADER, signed.nonce)
        .header(INTEGRATION_SIGNATURE_HEADER, signed.signature)
        .header("x-trace-id", request.trace_id.clone())
        .json(&request)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("core delivery endpoint returned {status}: {body}"));
    }

    let delivery: InboundDeliveryResponse = response.json().await?;
    if !delivery.accepted {
        observability::record_inbound_delivery("failed");
        return Err(anyhow!(
            "core delivery rejected inbound delivery: {}",
            delivery.detail.unwrap_or_else(|| "no detail".to_string())
        ));
    }
    observability::record_inbound_delivery("relayed");
    info!(
        trace_id = %request.trace_id,
        accepted = delivery.accepted,
        delivered_mailboxes = delivery.delivered_mailboxes.len(),
        internet_message_id = request.internet_message_id.as_deref().unwrap_or(""),
        "inbound message delivered to lpe core"
    );
    Ok(delivery)
}

async fn relay_message(
    config: &RuntimeConfig,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
    _last_attempt_error: Option<&str>,
) -> OutboundExecution {
    if config.mutual_tls_required {
        return OutboundExecution {
            status: TransportDeliveryStatus::Failed,
            detail: Some(
                "mutual TLS relay is configured but not implemented in LPE-CT v1".to_string(),
            ),
            remote_message_ref: None,
            retry: None,
            dsn: None,
            technical: Some(TransportTechnicalStatus {
                phase: "connect".to_string(),
                smtp_code: None,
                enhanced_code: None,
                remote_host: route.relay_target.clone(),
                detail: Some(
                    "mutual TLS relay is configured but not implemented in LPE-CT v1".to_string(),
                ),
            }),
            route: Some(TransportRouteDecision {
                rule_id: route.rule_id.clone(),
                relay_target: route.relay_target.clone(),
                queue: "held".to_string(),
            }),
            throttle: None,
        };
    }

    let mut targets = Vec::new();
    if let Some(target) = route.relay_target.clone() {
        targets.push(target);
    }
    for candidate in [&config.primary_upstream, &config.secondary_upstream] {
        let candidate = candidate.trim();
        if !candidate.is_empty() && !targets.iter().any(|existing| existing == candidate) {
            targets.push(candidate.to_string());
        }
    }

    let mut last_error = None;
    for target in targets {
        match relay_message_to_target(&target, message, route, attempt_count).await {
            Ok(execution) => return execution,
            Err(error) => last_error = Some((target, error)),
        }
    }

    let (target, error) =
        last_error.unwrap_or_else(|| ("".to_string(), anyhow!("no relay target configured")));
    let detail = error.to_string();
    let status = if is_permanent_relay_error(&detail) {
        TransportDeliveryStatus::Failed
    } else {
        TransportDeliveryStatus::Deferred
    };
    let retry = if status == TransportDeliveryStatus::Deferred {
        let retry_after = retry_after_seconds(300, attempt_count);
        Some(TransportRetryAdvice {
            retry_after_seconds: retry_after,
            policy: "connect-backoff".to_string(),
            reason: Some(detail.clone()),
        })
    } else {
        None
    };
    let dsn = if status == TransportDeliveryStatus::Deferred {
        Some(TransportDsnReport {
            action: "delayed".to_string(),
            status: "4.4.1".to_string(),
            diagnostic_code: Some(format!("smtp; {detail}")),
            remote_mta: if target.is_empty() {
                None
            } else {
                Some(target.clone())
            },
        })
    } else {
        None
    };
    OutboundExecution {
        status: status.clone(),
        detail: Some(detail.clone()),
        remote_message_ref: None,
        retry,
        dsn,
        technical: Some(TransportTechnicalStatus {
            phase: "connect".to_string(),
            smtp_code: None,
            enhanced_code: None,
            remote_host: if target.is_empty() {
                route.relay_target.clone()
            } else {
                Some(target.clone())
            },
            detail: Some(detail),
        }),
        route: Some(TransportRouteDecision {
            rule_id: route.rule_id.clone(),
            relay_target: if target.is_empty() {
                route.relay_target.clone()
            } else {
                Some(target)
            },
            queue: default_queue_for_status(&status).to_string(),
        }),
        throttle: None,
    }
}

async fn relay_message_to_target(
    target: &str,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
) -> Result<OutboundExecution> {
    let address = normalize_smtp_target(target);
    let stream = TcpStream::connect(&address)
        .await
        .with_context(|| format!("unable to connect to relay target {address}"))?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    expect_smtp(&mut reader, 220).await?;
    smtp_command(&mut reader, &mut writer, "EHLO lpe-ct", 250).await?;
    smtp_command(
        &mut reader,
        &mut writer,
        &format!("MAIL FROM:<{}>", message.mail_from),
        250,
    )
    .await?;
    for recipient in &message.rcpt_to {
        let reply = smtp_command_reply(
            &mut reader,
            &mut writer,
            &format!("RCPT TO:<{}>", recipient),
        )
        .await?;
        if !(reply.code == 250 || reply.code == 251) {
            let status = if reply.code >= 500 {
                TransportDeliveryStatus::Bounced
            } else {
                TransportDeliveryStatus::Deferred
            };
            let enhanced = parse_enhanced_status(&reply.message);
            return Ok(OutboundExecution {
                status: status.clone(),
                detail: Some(reply.message.clone()),
                remote_message_ref: None,
                retry: if status == TransportDeliveryStatus::Deferred {
                    let retry_after = retry_after_seconds(300, attempt_count);
                    Some(TransportRetryAdvice {
                        retry_after_seconds: retry_after,
                        policy: "remote-smtp".to_string(),
                        reason: Some(reply.message.clone()),
                    })
                } else {
                    None
                },
                dsn: Some(TransportDsnReport {
                    action: if status == TransportDeliveryStatus::Bounced {
                        "failed".to_string()
                    } else {
                        "delayed".to_string()
                    },
                    status: enhanced.clone().unwrap_or_else(|| {
                        if status == TransportDeliveryStatus::Bounced {
                            "5.1.1".to_string()
                        } else {
                            "4.4.1".to_string()
                        }
                    }),
                    diagnostic_code: Some(format!("smtp; {}", reply.message)),
                    remote_mta: Some(address.clone()),
                }),
                technical: Some(TransportTechnicalStatus {
                    phase: "rcpt-to".to_string(),
                    smtp_code: Some(reply.code),
                    enhanced_code: enhanced,
                    remote_host: Some(address.clone()),
                    detail: Some(reply.message.clone()),
                }),
                route: Some(TransportRouteDecision {
                    rule_id: route.rule_id.clone(),
                    relay_target: Some(target.to_string()),
                    queue: default_queue_for_status(&status).to_string(),
                }),
                throttle: None,
            });
        }
    }
    let data_reply = smtp_command_reply(&mut reader, &mut writer, "DATA").await?;
    if data_reply.code != 354 {
        let enhanced = parse_enhanced_status(&data_reply.message);
        return Ok(OutboundExecution {
            status: TransportDeliveryStatus::Deferred,
            detail: Some(data_reply.message.clone()),
            remote_message_ref: None,
            retry: {
                let retry_after = retry_after_seconds(300, attempt_count);
                Some(TransportRetryAdvice {
                    retry_after_seconds: retry_after,
                    policy: "remote-smtp".to_string(),
                    reason: Some(data_reply.message.clone()),
                })
            },
            dsn: Some(TransportDsnReport {
                action: "delayed".to_string(),
                status: enhanced.clone().unwrap_or_else(|| "4.3.0".to_string()),
                diagnostic_code: Some(format!("smtp; {}", data_reply.message)),
                remote_mta: Some(address.clone()),
            }),
            technical: Some(TransportTechnicalStatus {
                phase: "data".to_string(),
                smtp_code: Some(data_reply.code),
                enhanced_code: enhanced,
                remote_host: Some(address.clone()),
                detail: Some(data_reply.message.clone()),
            }),
            route: Some(TransportRouteDecision {
                rule_id: route.rule_id.clone(),
                relay_target: Some(target.to_string()),
                queue: "deferred".to_string(),
            }),
            throttle: None,
        });
    }
    writer.write_all(&message.data).await?;
    if !message.data.ends_with(b"\r\n") {
        writer.write_all(b"\r\n").await?;
    }
    writer.write_all(b".\r\n").await?;
    let final_reply = read_smtp_reply(&mut reader).await?;
    writer.write_all(b"QUIT\r\n").await?;
    if final_reply.code != 250 {
        let status = if final_reply.code >= 500 {
            TransportDeliveryStatus::Bounced
        } else {
            TransportDeliveryStatus::Deferred
        };
        let enhanced = parse_enhanced_status(&final_reply.message);
        return Ok(OutboundExecution {
            status: status.clone(),
            detail: Some(final_reply.message.clone()),
            remote_message_ref: None,
            retry: if status == TransportDeliveryStatus::Deferred {
                let retry_after = retry_after_seconds(300, attempt_count);
                Some(TransportRetryAdvice {
                    retry_after_seconds: retry_after,
                    policy: "remote-smtp".to_string(),
                    reason: Some(final_reply.message.clone()),
                })
            } else {
                None
            },
            dsn: Some(TransportDsnReport {
                action: if status == TransportDeliveryStatus::Bounced {
                    "failed".to_string()
                } else {
                    "delayed".to_string()
                },
                status: enhanced.clone().unwrap_or_else(|| {
                    if status == TransportDeliveryStatus::Bounced {
                        "5.0.0".to_string()
                    } else {
                        "4.0.0".to_string()
                    }
                }),
                diagnostic_code: Some(format!("smtp; {}", final_reply.message)),
                remote_mta: Some(address.clone()),
            }),
            technical: Some(TransportTechnicalStatus {
                phase: "final-response".to_string(),
                smtp_code: Some(final_reply.code),
                enhanced_code: enhanced,
                remote_host: Some(address.clone()),
                detail: Some(final_reply.message.clone()),
            }),
            route: Some(TransportRouteDecision {
                rule_id: route.rule_id.clone(),
                relay_target: Some(target.to_string()),
                queue: default_queue_for_status(&status).to_string(),
            }),
            throttle: None,
        });
    }

    Ok(OutboundExecution {
        status: TransportDeliveryStatus::Relayed,
        detail: None,
        remote_message_ref: Some(final_reply.message.clone()),
        retry: None,
        dsn: None,
        technical: Some(TransportTechnicalStatus {
            phase: "final-response".to_string(),
            smtp_code: Some(final_reply.code),
            enhanced_code: parse_enhanced_status(&final_reply.message),
            remote_host: Some(address.clone()),
            detail: Some(final_reply.message.clone()),
        }),
        route: Some(TransportRouteDecision {
            rule_id: route.rule_id.clone(),
            relay_target: Some(target.to_string()),
            queue: "sent".to_string(),
        }),
        throttle: None,
    })
}

async fn smtp_command(
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
    command: &str,
    expected: u16,
) -> Result<()> {
    writer.write_all(command.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    expect_smtp(reader, expected).await
}

async fn smtp_command_reply(
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
    command: &str,
) -> Result<SmtpReply> {
    writer.write_all(command.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    read_smtp_reply(reader).await
}

async fn expect_smtp(reader: &mut BufReader<OwnedReadHalf>, expected: u16) -> Result<()> {
    let reply = read_smtp_reply(reader).await?;
    if reply.code == expected {
        Ok(())
    } else {
        Err(anyhow!("unexpected SMTP response: {}", reply.message))
    }
}

async fn read_smtp_reply(reader: &mut BufReader<OwnedReadHalf>) -> Result<SmtpReply> {
    let mut line = String::new();
    let mut message = String::new();
    let code = loop {
        line.clear();
        reader.read_line(&mut line).await?;
        if line.len() < 3 {
            return Err(anyhow!("invalid SMTP response"));
        }
        let code = line[0..3].parse::<u16>().unwrap_or(0);
        let more = line.as_bytes().get(3) == Some(&b'-');
        let trimmed = line.trim_end().to_string();
        if !message.is_empty() {
            message.push('\n');
        }
        message.push_str(&trimmed);
        if !more {
            break code;
        }
    };

    Ok(SmtpReply { code, message })
}

async fn read_smtp_data(reader: &mut BufReader<OwnedReadHalf>, max_mb: u32) -> Result<Vec<u8>> {
    let max_bytes = max_mb.max(1) as usize * 1024 * 1024;
    let mut data = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).await? == 0 {
            return Err(anyhow!("client closed during DATA"));
        }
        if line == b".\r\n" || line == b".\n" {
            break;
        }
        if line.starts_with(b"..") {
            data.extend_from_slice(&line[1..]);
        } else {
            data.extend_from_slice(&line);
        }
        if data.len() > max_bytes {
            return Err(anyhow!("message exceeds configured maximum size"));
        }
    }
    Ok(data)
}

async fn write_smtp(writer: &mut OwnedWriteHalf, line: &str) -> Result<()> {
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    Ok(())
}

async fn persist_message(spool_dir: &Path, queue: &str, message: &QueuedMessage) -> Result<()> {
    let destination = spool_path(spool_dir, queue, &message.id);
    let temp_path = spool_dir.join(queue).join(format!("{}.tmp", message.id));
    tokio::fs::write(&temp_path, serde_json::to_vec_pretty(message)?).await?;
    tokio::fs::rename(&temp_path, &destination).await?;
    Ok(())
}

async fn move_message(
    spool_dir: &Path,
    message: &QueuedMessage,
    from: &str,
    to: &str,
) -> Result<()> {
    persist_message(spool_dir, to, message).await?;
    let _ = tokio::fs::remove_file(spool_path(spool_dir, from, &message.id)).await;
    Ok(())
}

fn append_transport_audit(spool_dir: &Path, queue: &str, message: &QueuedMessage) -> Result<()> {
    let event = TransportAuditEvent {
        timestamp: current_timestamp(),
        trace_id: message.id.clone(),
        direction: message.direction.clone(),
        queue: queue.to_string(),
        status: message.status.clone(),
        peer: message.peer.clone(),
        mail_from: message.mail_from.clone(),
        rcpt_to: message.rcpt_to.clone(),
        subject: parse_rfc822_header_value(&message.data, "subject").unwrap_or_default(),
        reason: message.relay_error.clone(),
        route_target: message
            .route
            .as_ref()
            .and_then(|route| route.relay_target.clone()),
        remote_message_ref: message.remote_message_ref.clone(),
        spam_score: message.spam_score,
        security_score: message.security_score,
        reputation_score: message.reputation_score,
    };
    let line = format!("{}\n", serde_json::to_string(&event)?);
    let path = spool_dir.join("policy").join("transport-audit.jsonl");
    let mut file = fs::OpenOptions::new().create(true).append(true).open(path)?;
    use std::io::Write;
    file.write_all(line.as_bytes())?;
    Ok(())
}

fn spool_path(spool_dir: &Path, queue: &str, id: &str) -> PathBuf {
    spool_dir.join(queue).join(format!("{id}.json"))
}

fn count_queue(spool_dir: &Path, queue: &str) -> Result<u32> {
    let path = spool_dir.join(queue);
    if !path.exists() {
        return Ok(0);
    }
    Ok(fs::read_dir(path)?
        .filter_map(std::result::Result::ok)
        .count() as u32)
}

pub(crate) fn list_quarantine_items(spool_dir: &Path) -> Result<Vec<QuarantineSummary>> {
    let mut items = Vec::new();
    for entry in fs::read_dir(spool_dir.join("quarantine"))? {
        let entry = entry?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let message = load_message_from_path(&entry.path())?;
        items.push(QuarantineSummary {
            trace_id: message.id.clone(),
            queue: "quarantine".to_string(),
            direction: message.direction.clone(),
            status: message.status.clone(),
            received_at: message.received_at.clone(),
            mail_from: message.mail_from.clone(),
            rcpt_to: message.rcpt_to.clone(),
            subject: parse_rfc822_header_value(&message.data, "subject").unwrap_or_default(),
            reason: message.relay_error.clone(),
            spam_score: message.spam_score,
            security_score: message.security_score,
            reputation_score: message.reputation_score,
            route_target: message
                .route
                .as_ref()
                .and_then(|route| route.relay_target.clone()),
        });
    }
    items.sort_by(|left, right| right.received_at.cmp(&left.received_at));
    Ok(items)
}

pub(crate) fn load_trace_details(spool_dir: &Path, trace_id: &str) -> Result<Option<TraceDetails>> {
    let Some((queue, message)) = find_message(spool_dir, trace_id)? else {
        return Ok(None);
    };
    Ok(Some(trace_details_from_message(&queue, &message)))
}

pub(crate) async fn retry_trace(
    spool_dir: &Path,
    trace_id: &str,
) -> Result<Option<TraceActionResult>> {
    transition_trace(spool_dir, trace_id, TraceAction::Retry).await
}

pub(crate) async fn release_trace(
    spool_dir: &Path,
    trace_id: &str,
) -> Result<Option<TraceActionResult>> {
    transition_trace(spool_dir, trace_id, TraceAction::Release).await
}

fn trace_details_from_message(queue: &str, message: &QueuedMessage) -> TraceDetails {
    TraceDetails {
        trace_id: message.id.clone(),
        queue: queue.to_string(),
        direction: message.direction.clone(),
        status: message.status.clone(),
        received_at: message.received_at.clone(),
        peer: message.peer.clone(),
        helo: message.helo.clone(),
        mail_from: message.mail_from.clone(),
        rcpt_to: message.rcpt_to.clone(),
        subject: parse_rfc822_header_value(&message.data, "subject").unwrap_or_default(),
        internet_message_id: parse_rfc822_header_value(&message.data, "message-id"),
        reason: message.relay_error.clone(),
        remote_message_ref: message.remote_message_ref.clone(),
        spam_score: message.spam_score,
        security_score: message.security_score,
        reputation_score: message.reputation_score,
        technical_status: message.technical_status.clone(),
        dsn: message.dsn.clone(),
        route: message.route.clone(),
        throttle: message.throttle.clone(),
        decision_trace: message.decision_trace.clone(),
    }
}

fn load_message_from_path(path: &Path) -> Result<QueuedMessage> {
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

fn find_message(spool_dir: &Path, trace_id: &str) -> Result<Option<(String, QueuedMessage)>> {
    for queue in SPOOL_QUEUES {
        let path = spool_path(spool_dir, queue, trace_id);
        if path.exists() {
            return Ok(Some((queue.to_string(), load_message_from_path(&path)?)));
        }
    }
    Ok(None)
}

#[derive(Clone, Copy)]
enum TraceAction {
    Retry,
    Release,
}

async fn transition_trace(
    spool_dir: &Path,
    trace_id: &str,
    action: TraceAction,
) -> Result<Option<TraceActionResult>> {
    let Some((queue, mut message)) = find_message(spool_dir, trace_id)? else {
        return Ok(None);
    };
    let Some(target_queue) = transition_target(&queue, &message.direction, action) else {
        return Ok(Some(TraceActionResult {
            trace_id: message.id.clone(),
            from_queue: queue,
            to_queue: String::new(),
            status: message.status.clone(),
            detail: "trace is not eligible for the requested action".to_string(),
        }));
    };
    message.status = target_queue.to_string();
    message.relay_error = None;
    message.decision_trace.push(DecisionTraceEntry {
        stage: "operator-action".to_string(),
        outcome: match action {
            TraceAction::Retry => "retry".to_string(),
            TraceAction::Release => "release".to_string(),
        },
        detail: format!("operator moved trace into {target_queue}"),
    });
    move_message(spool_dir, &message, &queue, target_queue).await?;
    Ok(Some(TraceActionResult {
        trace_id: message.id.clone(),
        from_queue: queue,
        to_queue: target_queue.to_string(),
        status: message.status.clone(),
        detail: format!("trace moved into {target_queue}"),
    }))
}

fn transition_target(queue: &str, direction: &str, action: TraceAction) -> Option<&'static str> {
    match (queue, direction, action) {
        ("deferred", "outbound", TraceAction::Retry) => Some("outbound"),
        ("held", "outbound", TraceAction::Retry) => Some("outbound"),
        ("deferred", "inbound", TraceAction::Retry) => Some("incoming"),
        ("held", "inbound", TraceAction::Retry) => Some("incoming"),
        ("quarantine", "outbound", TraceAction::Release) => Some("outbound"),
        ("quarantine", "inbound", TraceAction::Release) => Some("incoming"),
        ("held", "outbound", TraceAction::Release) => Some("outbound"),
        ("held", "inbound", TraceAction::Release) => Some("incoming"),
        _ => None,
    }
}

fn runtime_config(state_file: &Path) -> Result<RuntimeConfig> {
    let raw = fs::read_to_string(state_file)
        .with_context(|| format!("unable to read state file {}", state_file.display()))?;
    let value = serde_json::from_str::<Value>(&raw)?;
    Ok(RuntimeConfig {
        primary_upstream: string_at(&value, &["relay", "primary_upstream"]),
        secondary_upstream: string_at(&value, &["relay", "secondary_upstream"]),
        core_delivery_base_url: string_at(&value, &["relay", "core_delivery_base_url"]),
        mutual_tls_required: bool_at(&value, &["relay", "mutual_tls_required"], false),
        fallback_to_hold_queue: bool_at(&value, &["relay", "fallback_to_hold_queue"], false),
        drain_mode: bool_at(&value, &["policies", "drain_mode"], false),
        quarantine_enabled: bool_at(&value, &["policies", "quarantine_enabled"], true),
        greylisting_enabled: bool_at(&value, &["policies", "greylisting_enabled"], true),
        antivirus_enabled: bool_at(&value, &["policies", "antivirus_enabled"], false),
        antivirus_fail_closed: bool_at(&value, &["policies", "antivirus_fail_closed"], true),
        antivirus_provider_chain: strings_at(
            &value,
            &["policies", "antivirus_provider_chain"],
            &["takeri"],
        ),
        antivirus_providers: load_antivirus_providers(&strings_at(
            &value,
            &["policies", "antivirus_provider_chain"],
            &["takeri"],
        )),
        bayespam_enabled: bool_at(&value, &["policies", "bayespam_enabled"], true),
        bayespam_auto_learn: bool_at(&value, &["policies", "bayespam_auto_learn"], true),
        bayespam_score_weight: f32_at(&value, &["policies", "bayespam_score_weight"], 6.0),
        bayespam_min_token_length: u32_at(&value, &["policies", "bayespam_min_token_length"], 3),
        bayespam_max_tokens: u32_at(&value, &["policies", "bayespam_max_tokens"], 256),
        require_spf: bool_at(&value, &["policies", "require_spf"], true),
        require_dkim_alignment: bool_at(&value, &["policies", "require_dkim_alignment"], false),
        require_dmarc_enforcement: bool_at(
            &value,
            &["policies", "require_dmarc_enforcement"],
            true,
        ),
        defer_on_auth_tempfail: bool_at(&value, &["policies", "defer_on_auth_tempfail"], true),
        dnsbl_enabled: bool_at(&value, &["policies", "dnsbl_enabled"], true),
        dnsbl_zones: strings_at(
            &value,
            &["policies", "dnsbl_zones"],
            &["zen.spamhaus.org", "bl.spamcop.net"],
        ),
        reputation_enabled: bool_at(&value, &["policies", "reputation_enabled"], true),
        reputation_quarantine_threshold: i32_at(
            &value,
            &["policies", "reputation_quarantine_threshold"],
            -4,
        ),
        reputation_reject_threshold: i32_at(
            &value,
            &["policies", "reputation_reject_threshold"],
            -8,
        ),
        spam_quarantine_threshold: f32_at(&value, &["policies", "spam_quarantine_threshold"], 5.0),
        spam_reject_threshold: f32_at(&value, &["policies", "spam_reject_threshold"], 9.0),
        max_message_size_mb: u32_at(&value, &["policies", "max_message_size_mb"], 64),
        max_concurrent_sessions: u32_at(&value, &["network", "max_concurrent_sessions"], 250)
            .max(1),
        routing_rules: routing_rules_at(&value),
        throttle_enabled: bool_at(&value, &["throttling", "enabled"], true),
        throttle_rules: throttle_rules_at(&value),
        local_db_enabled: bool_at(
            &value,
            &["local_data_stores", "dedicated_postgres", "enabled"],
            true,
        ),
        local_db_url: env::var("LPE_CT_LOCAL_DB_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
    })
}

fn quarantine_metadata(spool_dir: &Path, message: &QueuedMessage) -> QuarantineMetadata {
    QuarantineMetadata {
        trace_id: message.id.clone(),
        direction: message.direction.clone(),
        status: message.status.clone(),
        received_at: message.received_at.clone(),
        peer: message.peer.clone(),
        helo: message.helo.clone(),
        mail_from: message.mail_from.clone(),
        rcpt_to: message.rcpt_to.clone(),
        subject: parse_rfc822_header_value(&message.data, "subject").unwrap_or_default(),
        internet_message_id: parse_rfc822_header_value(&message.data, "message-id"),
        spool_path: spool_path(spool_dir, "quarantine", &message.id)
            .display()
            .to_string(),
        reason: message.relay_error.clone(),
        spam_score: message.spam_score,
        security_score: message.security_score,
        reputation_score: message.reputation_score,
        dnsbl_hits: message.dnsbl_hits.clone(),
        auth_summary: message.auth_summary.clone(),
        decision_trace: message.decision_trace.clone(),
        magika_summary: message.magika_summary.clone(),
        magika_decision: message.magika_decision.clone(),
    }
}

fn retry_after_seconds(base: u32, attempt_count: u32) -> u32 {
    let multiplier = 2u32.saturating_pow(attempt_count.min(4));
    base.max(1).saturating_mul(multiplier).min(3600)
}

async fn persist_quarantine_metadata(
    spool_dir: &Path,
    config: &RuntimeConfig,
    message: &QueuedMessage,
) -> Result<()> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(());
    };

    let metadata = quarantine_metadata(spool_dir, message);
    sqlx::query(
        r#"
        INSERT INTO quarantine_messages (
            trace_id, direction, status, received_at, peer, helo, mail_from, rcpt_to,
            subject, internet_message_id, spool_path, reason, spam_score, security_score,
            reputation_score, dnsbl_hits, auth_summary, decision_trace, magika_summary,
            magika_decision
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14,
            $15, $16, $17, $18, $19, $20
        )
        ON CONFLICT (trace_id) DO UPDATE SET
            status = EXCLUDED.status,
            received_at = EXCLUDED.received_at,
            peer = EXCLUDED.peer,
            helo = EXCLUDED.helo,
            mail_from = EXCLUDED.mail_from,
            rcpt_to = EXCLUDED.rcpt_to,
            subject = EXCLUDED.subject,
            internet_message_id = EXCLUDED.internet_message_id,
            spool_path = EXCLUDED.spool_path,
            reason = EXCLUDED.reason,
            spam_score = EXCLUDED.spam_score,
            security_score = EXCLUDED.security_score,
            reputation_score = EXCLUDED.reputation_score,
            dnsbl_hits = EXCLUDED.dnsbl_hits,
            auth_summary = EXCLUDED.auth_summary,
            decision_trace = EXCLUDED.decision_trace,
            magika_summary = EXCLUDED.magika_summary,
            magika_decision = EXCLUDED.magika_decision,
            updated_at = NOW()
        "#,
    )
    .bind(&metadata.trace_id)
    .bind(&metadata.direction)
    .bind(&metadata.status)
    .bind(&metadata.received_at)
    .bind(&metadata.peer)
    .bind(&metadata.helo)
    .bind(&metadata.mail_from)
    .bind(Json(metadata.rcpt_to))
    .bind(&metadata.subject)
    .bind(&metadata.internet_message_id)
    .bind(&metadata.spool_path)
    .bind(&metadata.reason)
    .bind(metadata.spam_score)
    .bind(metadata.security_score)
    .bind(metadata.reputation_score)
    .bind(Json(metadata.dnsbl_hits))
    .bind(Json(metadata.auth_summary))
    .bind(Json(metadata.decision_trace))
    .bind(&metadata.magika_summary)
    .bind(&metadata.magika_decision)
    .execute(pool)
    .await?;

    Ok(())
}

async fn persist_quarantine_metadata_or_warn(
    spool_dir: &Path,
    config: &RuntimeConfig,
    message: &QueuedMessage,
) {
    if let Err(error) = persist_quarantine_metadata(spool_dir, config, message).await {
        warn!(
            trace_id = %message.id,
            error = %error,
            "unable to persist quarantine metadata in local PostgreSQL"
        );
    }
}

fn routing_rules_at(value: &Value) -> Vec<OutboundRoutingRule> {
    value
        .get("routing")
        .and_then(|routing| routing.get("rules"))
        .and_then(Value::as_array)
        .map(|rules| {
            rules
                .iter()
                .filter_map(|rule| {
                    Some(OutboundRoutingRule {
                        id: rule.get("id")?.as_str()?.to_string(),
                        sender_domain: rule
                            .get("sender_domain")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        recipient_domain: rule
                            .get("recipient_domain")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        relay_target: rule.get("relay_target")?.as_str()?.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn throttle_rules_at(value: &Value) -> Vec<OutboundThrottleRule> {
    value
        .get("throttling")
        .and_then(|throttling| throttling.get("rules"))
        .and_then(Value::as_array)
        .map(|rules| {
            rules
                .iter()
                .filter_map(|rule| {
                    Some(OutboundThrottleRule {
                        id: rule.get("id")?.as_str()?.to_string(),
                        scope: rule.get("scope")?.as_str()?.to_string(),
                        recipient_domain: rule
                            .get("recipient_domain")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        sender_domain: rule
                            .get("sender_domain")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        max_messages: rule.get("max_messages")?.as_u64()? as u32,
                        window_seconds: rule.get("window_seconds")?.as_u64()? as u32,
                        retry_after_seconds: rule.get("retry_after_seconds")?.as_u64()? as u32,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn string_at(value: &Value, path: &[&str]) -> String {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn bool_at(value: &Value, path: &[&str], fallback: bool) -> bool {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_bool)
        .unwrap_or(fallback)
}

fn u32_at(value: &Value, path: &[&str], fallback: u32) -> u32 {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_u64)
        .map(|value| value as u32)
        .unwrap_or(fallback)
}

fn f32_at(value: &Value, path: &[&str], fallback: f32) -> f32 {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_f64)
        .map(|value| value as f32)
        .unwrap_or(fallback)
}

fn i32_at(value: &Value, path: &[&str], fallback: i32) -> i32 {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_i64)
        .and_then(|parsed| i32::try_from(parsed).ok())
        .unwrap_or(fallback)
}

fn strings_at(value: &Value, path: &[&str], fallback: &[&str]) -> Vec<String> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| fallback.iter().map(|value| value.to_string()).collect())
}

fn should_quarantine(data: &[u8]) -> bool {
    String::from_utf8_lossy(data).lines().any(|line| {
        let lower = line.to_ascii_lowercase();
        lower.starts_with("x-lpe-ct-quarantine: yes") || lower.contains("[quarantine]")
    })
}

fn normalize_smtp_target(target: &str) -> String {
    target
        .trim()
        .trim_start_matches("smtp://")
        .trim_start_matches("tcp://")
        .to_string()
}

fn compose_rfc822_message(payload: &OutboundMessageHandoffRequest) -> Vec<u8> {
    let mut lines = Vec::new();
    lines.push(format!(
        "From: {}",
        format_address(&payload.from_address, payload.from_display.as_deref())
    ));
    if !payload.to.is_empty() {
        lines.push(format!(
            "To: {}",
            payload
                .to
                .iter()
                .map(|recipient| format_address(
                    &recipient.address,
                    recipient.display_name.as_deref()
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !payload.cc.is_empty() {
        lines.push(format!(
            "Cc: {}",
            payload
                .cc
                .iter()
                .map(|recipient| format_address(
                    &recipient.address,
                    recipient.display_name.as_deref()
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.push(format!("Subject: {}", payload.subject));
    lines.push(format!(
        "Message-Id: {}",
        payload
            .internet_message_id
            .clone()
            .unwrap_or_else(|| format!("<{}@lpe.local>", payload.message_id))
    ));
    lines.push("MIME-Version: 1.0".to_string());
    if let Some(html) = payload
        .body_html_sanitized
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let boundary = format!("lpe-alt-{}", payload.message_id);
        lines.push(format!(
            "Content-Type: multipart/alternative; boundary=\"{boundary}\""
        ));
        lines.push(String::new());
        lines.push(format!("--{boundary}"));
        lines.push("Content-Type: text/plain; charset=utf-8".to_string());
        lines.push("Content-Transfer-Encoding: quoted-printable".to_string());
        lines.push(String::new());
        lines.push(encode_quoted_printable(&payload.body_text));
        lines.push(format!("--{boundary}"));
        lines.push("Content-Type: text/html; charset=utf-8".to_string());
        lines.push("Content-Transfer-Encoding: quoted-printable".to_string());
        lines.push(String::new());
        lines.push(encode_quoted_printable(html));
        lines.push(format!("--{boundary}--"));
    } else {
        lines.push("Content-Type: text/plain; charset=utf-8".to_string());
        lines.push("Content-Transfer-Encoding: quoted-printable".to_string());
        lines.push(String::new());
        lines.push(encode_quoted_printable(&payload.body_text));
    }
    lines.join("\r\n").into_bytes()
}

fn format_address(address: &str, display_name: Option<&str>) -> String {
    match display_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(display_name) => format!("{display_name} <{address}>"),
        None => address.to_string(),
    }
}

fn encode_quoted_printable(value: &str) -> String {
    let mut encoded = String::new();
    let mut line_len = 0usize;
    for &byte in value.as_bytes() {
        match byte {
            b'\r' => {}
            b'\n' => {
                encoded.push_str("\r\n");
                line_len = 0;
            }
            b'\t' | b' ' | 33..=60 | 62..=126 => {
                if line_len >= 72 {
                    encoded.push_str("=\r\n");
                    line_len = 0;
                }
                encoded.push(byte as char);
                line_len += 1;
            }
            _ => {
                if line_len >= 70 {
                    encoded.push_str("=\r\n");
                    line_len = 0;
                }
                encoded.push_str(&format!("={byte:02X}"));
                line_len += 3;
            }
        }
    }
    encoded
}

fn is_permanent_relay_error(detail: &str) -> bool {
    let lower = detail.to_ascii_lowercase();
    lower.contains("no relay target configured")
        || lower.contains("mutual tls relay is configured but not implemented")
}

fn parse_enhanced_status(detail: &str) -> Option<String> {
    detail
        .split_whitespace()
        .map(|token| token.trim_matches(|ch: char| matches!(ch, ';' | ',' | ':')))
        .find(|token| {
            let mut parts = token.split('.');
            matches!(
                (parts.next(), parts.next(), parts.next(), parts.next()),
                (Some(a), Some(b), Some(c), None)
                    if a.chars().all(|ch| ch.is_ascii_digit())
                        && b.chars().all(|ch| ch.is_ascii_digit())
                        && c.chars().all(|ch| ch.is_ascii_digit())
            )
        })
        .map(ToString::to_string)
}

fn message_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("lpe-ct-{prefix}-{nanos}-{}", std::process::id())
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
        apply_authentication_scores, classify_inbound_message, compose_rfc822_message,
        dkim_disposition, dnsbl_query_name, encode_quoted_printable, evaluate_greylisting,
        handle_smtp_session, initialize_spool, load_antivirus_providers, load_bayespam_corpus,
        load_reputation_score, parse_antivirus_output, parse_peer_ip, process_outbound_handoff,
        receive_message, receive_message_with_validator, retry_after_seconds, score_bayespam,
        spf_disposition, stable_key_id, summarize_dkim, summarize_dmarc, summarize_spf,
        train_bayespam, unix_now, update_reputation, AntivirusProviderConfig,
        AntivirusProviderDecision, AuthSummary, AuthenticationAssessment, BayesLabel,
        DkimDisposition, FilterAction, GreylistEntry, OutboundRoutingRule, OutboundThrottleRule,
        QueuedMessage, RuntimeConfig, SpfDisposition,
    };
    use crate::env_test_lock;
    use axum::{routing::post, Json, Router};
    use email_auth::{dkim::DkimResult, dmarc::Disposition as DmarcDisposition, spf::SpfResult};
    use lpe_domain::{
        InboundDeliveryRequest, InboundDeliveryResponse, OutboundMessageHandoffRequest,
        TransportDeliveryStatus, TransportRecipient,
    };
    use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
    use std::{
        net::IpAddr,
        net::SocketAddr,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
    };
    use uuid::Uuid;

    fn temp_dir(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("lpe-ct-{label}-{suffix}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn runtime_config(primary_upstream: String, core_delivery_base_url: String) -> RuntimeConfig {
        RuntimeConfig {
            primary_upstream,
            secondary_upstream: String::new(),
            core_delivery_base_url,
            mutual_tls_required: false,
            fallback_to_hold_queue: false,
            drain_mode: false,
            quarantine_enabled: true,
            greylisting_enabled: false,
            antivirus_enabled: false,
            antivirus_fail_closed: true,
            antivirus_provider_chain: vec!["takeri".to_string()],
            antivirus_providers: load_antivirus_providers(&["takeri".to_string()]),
            bayespam_enabled: true,
            bayespam_auto_learn: true,
            bayespam_score_weight: 6.0,
            bayespam_min_token_length: 3,
            bayespam_max_tokens: 256,
            require_spf: true,
            require_dkim_alignment: false,
            require_dmarc_enforcement: true,
            defer_on_auth_tempfail: true,
            dnsbl_enabled: false,
            dnsbl_zones: Vec::new(),
            reputation_enabled: true,
            reputation_quarantine_threshold: -4,
            reputation_reject_threshold: -8,
            spam_quarantine_threshold: 5.0,
            spam_reject_threshold: 9.0,
            max_message_size_mb: 16,
            max_concurrent_sessions: 250,
            routing_rules: Vec::new(),
            throttle_enabled: false,
            throttle_rules: Vec::new(),
            local_db_enabled: false,
            local_db_url: None,
        }
    }

    fn training_message(subject: &str, body: &str) -> QueuedMessage {
        QueuedMessage {
            id: format!("trace-{}", stable_key_id(&(subject, body))),
            direction: "inbound".to_string(),
            received_at: "unix:1".to_string(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "sender@example.test".to_string(),
            rcpt_to: vec!["dest@example.test".to_string()],
            status: "incoming".to_string(),
            relay_error: None,
            magika_summary: None,
            magika_decision: None,
            spam_score: 0.0,
            security_score: 0.0,
            reputation_score: 0,
            dnsbl_hits: Vec::new(),
            auth_summary: AuthSummary::default(),
            decision_trace: Vec::new(),
            remote_message_ref: None,
            technical_status: None,
            dsn: None,
            route: None,
            throttle: None,
            data: format!("Subject: {subject}\r\n\r\n{body}").into_bytes(),
        }
    }

    #[derive(Debug, Clone)]
    struct FakeDetector {
        detection: Result<MagikaDetection, String>,
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
            self.detection.clone().map_err(anyhow::Error::msg)
        }
    }

    #[tokio::test]
    async fn outbound_handoff_relays_message() {
        let spool = temp_dir("outbound-relay");
        initialize_spool(&spool).unwrap();
        let captured = Arc::new(Mutex::new(String::new()));
        let smtp_address = spawn_dummy_smtp(captured.clone()).await;

        let response = process_outbound_handoff(
            &spool,
            &runtime_config(smtp_address.clone(), "http://127.0.0.1:9".to_string()),
            OutboundMessageHandoffRequest {
                queue_id: Uuid::new_v4(),
                message_id: Uuid::new_v4(),
                account_id: Uuid::new_v4(),
                from_address: "sender@example.test".to_string(),
                from_display: Some("Sender".to_string()),
                sender_address: None,
                sender_display: None,
                sender_authorization_kind: "self".to_string(),
                to: vec![TransportRecipient {
                    address: "dest@example.test".to_string(),
                    display_name: Some("Dest".to_string()),
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Relay test".to_string(),
                body_text: "Body".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some("<relay@test>".to_string()),
                attempt_count: 0,
                last_attempt_error: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(response.status, TransportDeliveryStatus::Relayed);
        assert_eq!(
            response
                .route
                .as_ref()
                .and_then(|route| route.rule_id.as_deref()),
            None
        );
        assert_eq!(
            response
                .route
                .as_ref()
                .and_then(|route| route.relay_target.as_deref()),
            Some(smtp_address.as_str())
        );
        assert_eq!(
            response
                .technical
                .as_ref()
                .and_then(|status| status.smtp_code),
            Some(250)
        );
        assert!(spool
            .join("sent")
            .join(format!("{}.json", response.trace_id))
            .exists());
        let raw = captured.lock().unwrap().clone();
        assert!(raw.contains("Subject: Relay test"));
        assert!(raw.contains("Content-Type: text/plain; charset=utf-8"));
        assert!(raw.contains("Content-Transfer-Encoding: quoted-printable"));
    }

    #[tokio::test]
    #[ignore = "env-sensitive"]
    async fn smtp_session_rejects_when_ha_role_is_standby() {
        let _guard = env_test_lock();
        let spool = temp_dir("smtp-standby");
        initialize_spool(&spool).unwrap();
        let role_file = spool.join("ha-role");
        std::fs::write(&role_file, b"standby\n").unwrap();
        std::env::set_var("LPE_CT_HA_ROLE_FILE", &role_file);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let spool_for_server = spool.clone();
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_smtp_session(
                stream,
                peer,
                spool_for_server.join("state.json"),
                spool_for_server,
            )
            .await
            .unwrap();
        });

        let client = TcpStream::connect(address).await.unwrap();
        let mut reader = BufReader::new(client);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with("421 node role standby"));

        server.await.unwrap();
        std::env::remove_var("LPE_CT_HA_ROLE_FILE");
    }

    #[tokio::test]
    async fn outbound_handoff_quarantines_message() {
        let spool = temp_dir("outbound-quarantine");
        initialize_spool(&spool).unwrap();

        let response = process_outbound_handoff(
            &spool,
            &runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string()),
            OutboundMessageHandoffRequest {
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
                subject: "[quarantine] Test".to_string(),
                body_text: "Body".to_string(),
                body_html_sanitized: None,
                internet_message_id: None,
                attempt_count: 0,
                last_attempt_error: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(response.status, TransportDeliveryStatus::Quarantined);
        assert!(spool
            .join("quarantine")
            .join(format!("{}.json", response.trace_id))
            .exists());
    }

    #[tokio::test]
    async fn outbound_handoff_bounces_on_permanent_rcpt_failure() {
        let spool = temp_dir("outbound-bounce");
        initialize_spool(&spool).unwrap();
        let smtp_address = spawn_dummy_smtp_with_profile(DummySmtpProfile {
            rcpt_reply: "550 5.1.1 user unknown".to_string(),
            ..DummySmtpProfile::default()
        })
        .await;

        let response = process_outbound_handoff(
            &spool,
            &runtime_config(smtp_address.clone(), "http://127.0.0.1:9".to_string()),
            outbound_request("Bounce test"),
        )
        .await
        .unwrap();

        assert_eq!(response.status, TransportDeliveryStatus::Bounced);
        assert_eq!(
            response.dsn.as_ref().map(|dsn| dsn.status.as_str()),
            Some("5.1.1")
        );
        assert_eq!(
            response
                .technical
                .as_ref()
                .and_then(|status| status.smtp_code),
            Some(550)
        );
        assert!(spool
            .join("bounces")
            .join(format!("{}.json", response.trace_id))
            .exists());
    }

    #[tokio::test]
    async fn outbound_handoff_defers_when_local_throttle_hits() {
        let spool = temp_dir("outbound-throttle");
        initialize_spool(&spool).unwrap();
        let smtp_address = spawn_dummy_smtp(Arc::new(Mutex::new(String::new()))).await;
        let mut config = runtime_config(smtp_address, "http://127.0.0.1:9".to_string());
        config.throttle_enabled = true;
        config.throttle_rules = vec![OutboundThrottleRule {
            id: "recipient-domain".to_string(),
            scope: "recipient-domain".to_string(),
            recipient_domain: None,
            sender_domain: None,
            max_messages: 1,
            window_seconds: 300,
            retry_after_seconds: 120,
        }];

        let first = process_outbound_handoff(&spool, &config, outbound_request("First"))
            .await
            .unwrap();
        let second = process_outbound_handoff(&spool, &config, outbound_request("Second"))
            .await
            .unwrap();

        assert_eq!(first.status, TransportDeliveryStatus::Relayed);
        assert_eq!(second.status, TransportDeliveryStatus::Deferred);
        assert_eq!(
            second
                .throttle
                .as_ref()
                .map(|throttle| throttle.retry_after_seconds),
            Some(120)
        );
        assert_eq!(
            second.retry.as_ref().map(|retry| retry.policy.as_str()),
            Some("throttle")
        );
    }

    #[tokio::test]
    async fn outbound_handoff_uses_matching_routing_rule() {
        let spool = temp_dir("outbound-routing");
        initialize_spool(&spool).unwrap();
        let smtp_address = spawn_dummy_smtp(Arc::new(Mutex::new(String::new()))).await;
        let mut config =
            runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
        config.routing_rules = vec![OutboundRoutingRule {
            id: "example-route".to_string(),
            sender_domain: None,
            recipient_domain: Some("example.test".to_string()),
            relay_target: smtp_address.clone(),
        }];

        let response = process_outbound_handoff(&spool, &config, outbound_request("Routed"))
            .await
            .unwrap();

        assert_eq!(response.status, TransportDeliveryStatus::Relayed);
        assert_eq!(
            response
                .route
                .as_ref()
                .and_then(|route| route.rule_id.as_deref()),
            Some("example-route")
        );
        assert_eq!(
            response
                .route
                .as_ref()
                .and_then(|route| route.relay_target.as_deref()),
            Some(smtp_address.as_str())
        );
    }

    #[tokio::test]
    #[ignore = "env-sensitive"]
    async fn inbound_message_posts_to_core_delivery_api() {
        let _guard = env_test_lock();
        let spool = temp_dir("inbound-delivery");
        initialize_spool(&spool).unwrap();
        std::env::set_var(
            "LPE_INTEGRATION_SHARED_SECRET",
            "0123456789abcdef0123456789abcdef",
        );
        let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
        let core_base_url = spawn_dummy_core(captured.clone()).await;

        let message = receive_message(
            &spool,
            &runtime_config("127.0.0.1:9".to_string(), core_base_url),
            "127.0.0.1:2525".to_string(),
            "example.test".to_string(),
            "sender@example.test".to_string(),
            vec!["dest@example.test".to_string()],
            b"From: Sender <sender@example.test>\r\nSubject: Inbound\r\n\r\nBody".to_vec(),
        )
        .await
        .unwrap();

        assert_eq!(message.status, "sent");
        assert!(spool
            .join("sent")
            .join(format!("{}.json", message.id))
            .exists());
        let request = captured.lock().unwrap().clone().unwrap();
        assert_eq!(request.subject, "Inbound");
        assert_eq!(request.body_text, "Body");
        assert_eq!(request.rcpt_to, vec!["dest@example.test".to_string()]);
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
    }

    #[test]
    fn inbound_mismatch_is_rejected_before_delivery() {
        let validator = Validator::new(
            FakeDetector {
                detection: Ok(MagikaDetection {
                    label: "exe".to_string(),
                    mime_type: "application/x-msdownload".to_string(),
                    description: "Executable".to_string(),
                    group: "binary".to_string(),
                    extensions: vec!["exe".to_string()],
                    score: Some(0.99),
                }),
            },
            0.80,
        );
        let mime = concat!(
            "Content-Type: multipart/mixed; boundary=\"abc\"\r\n",
            "\r\n",
            "--abc\r\n",
            "Content-Type: application/pdf; name=\"invoice.pdf\"\r\n",
            "Content-Disposition: attachment; filename=\"invoice.pdf\"\r\n",
            "\r\n",
            "%PDF-1.7\r\n",
            "--abc--\r\n"
        );

        let outcome = classify_inbound_message(&validator, mime.as_bytes()).unwrap();
        assert!(matches!(outcome, super::InboundMagikaOutcome::Reject(_)));
    }

    #[tokio::test]
    async fn inbound_magika_failure_is_quarantined() {
        let spool = temp_dir("inbound-quarantine-magika");
        initialize_spool(&spool).unwrap();
        let validator = Validator::new(
            FakeDetector {
                detection: Err("binary unavailable".to_string()),
            },
            0.80,
        );

        let message = receive_message_with_validator(
            &validator,
            &spool,
            &runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string()),
            "127.0.0.1:2525".to_string(),
            "example.test".to_string(),
            "sender@example.test".to_string(),
            vec!["dest@example.test".to_string()],
            concat!(
                "Content-Type: multipart/mixed; boundary=\"abc\"\r\n",
                "\r\n",
                "--abc\r\n",
                "Content-Type: application/pdf; name=\"invoice.pdf\"\r\n",
                "Content-Disposition: attachment; filename=\"invoice.pdf\"\r\n",
                "\r\n",
                "%PDF-1.7\r\n",
                "--abc--\r\n"
            )
            .as_bytes()
            .to_vec(),
        )
        .await
        .unwrap();

        assert_eq!(message.status, "quarantined");
        assert!(message
            .magika_summary
            .as_deref()
            .unwrap_or_default()
            .contains("Magika validation failed"));
        assert!(spool
            .join("quarantine")
            .join(format!("{}.json", message.id))
            .exists());
    }

    #[test]
    fn outbound_handoff_builds_multipart_alternative_when_html_is_present() {
        let raw = String::from_utf8(compose_rfc822_message(&OutboundMessageHandoffRequest {
            queue_id: Uuid::nil(),
            message_id: Uuid::nil(),
            account_id: Uuid::nil(),
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
            subject: "HTML".to_string(),
            body_text: "Plain body".to_string(),
            body_html_sanitized: Some("<p>HTML body</p>".to_string()),
            internet_message_id: None,
            attempt_count: 0,
            last_attempt_error: None,
        }))
        .unwrap();

        assert!(raw.contains("Content-Type: multipart/alternative;"));
        assert!(raw.contains("Content-Type: text/plain; charset=utf-8"));
        assert!(raw.contains("Content-Type: text/html; charset=utf-8"));
        assert!(!raw.contains("\r\nBcc:"));
    }

    #[test]
    fn quoted_printable_encoder_handles_utf8_and_line_breaks() {
        let encoded = encode_quoted_printable("Bonjour équipe\nHTML");
        assert!(encoded.contains("=C3=A9"));
        assert!(encoded.contains("\r\n"));
    }

    #[tokio::test]
    #[ignore = "env-sensitive"]
    async fn inbound_message_keeps_non_utf8_raw_bytes() {
        let _guard = env_test_lock();
        let spool = temp_dir("inbound-non-utf8");
        initialize_spool(&spool).unwrap();
        std::env::set_var(
            "LPE_INTEGRATION_SHARED_SECRET",
            "0123456789abcdef0123456789abcdef",
        );
        let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
        let core_base_url = spawn_dummy_core(captured.clone()).await;
        let validator = Validator::new(
            FakeDetector {
                detection: Ok(MagikaDetection {
                    label: "bin".to_string(),
                    mime_type: "application/octet-stream".to_string(),
                    description: "Binary".to_string(),
                    group: "binary".to_string(),
                    extensions: vec!["bin".to_string()],
                    score: Some(0.99),
                }),
            },
            0.80,
        );
        let mut raw = b"From: Sender <sender@example.test>\r\nSubject: Binary\r\nContent-Type: multipart/mixed; boundary=\"b1\"\r\n\r\n--b1\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nVisible body\r\n--b1\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"blob.bin\"\r\n\r\n".to_vec();
        raw.extend_from_slice(&[0xff, 0xfe, 0x00, 0x41]);
        raw.extend_from_slice(b"\r\n--b1--\r\n");

        let message = receive_message_with_validator(
            &validator,
            &spool,
            &runtime_config("127.0.0.1:9".to_string(), core_base_url),
            "127.0.0.1:2525".to_string(),
            "example.test".to_string(),
            "sender@example.test".to_string(),
            vec!["dest@example.test".to_string()],
            raw.clone(),
        )
        .await
        .unwrap();

        assert_eq!(message.status, "sent");
        let request = captured.lock().unwrap().clone().unwrap();
        assert_eq!(request.body_text, "Visible body");
        assert_eq!(request.raw_message, raw);
        std::env::remove_var("LPE_INTEGRATION_SHARED_SECRET");
    }

    #[tokio::test]
    async fn greylisting_defers_first_triplet_then_allows_after_release_window() {
        let spool = temp_dir("greylisting");
        initialize_spool(&spool).unwrap();
        let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
        let ip: IpAddr = "192.0.2.45".parse().unwrap();
        let rcpt = vec!["dest@example.test".to_string()];

        let first = evaluate_greylisting(&spool, &config, ip, "sender@example.test", &rcpt)
            .await
            .unwrap();
        assert!(first.unwrap().contains("greylisted triplet"));

        let key = stable_key_id(&(
            ip,
            "sender@example.test".to_string(),
            "dest@example.test".to_string(),
        ));
        let path = spool.join("greylist").join(format!("{key}.json"));
        let mut entry: GreylistEntry =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        entry.release_after_unix = unix_now().saturating_sub(1);
        std::fs::write(&path, serde_json::to_string_pretty(&entry).unwrap()).unwrap();

        let second = evaluate_greylisting(&spool, &config, ip, "sender@example.test", &rcpt)
            .await
            .unwrap();
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn reputation_score_penalizes_quarantine_and_rejects() {
        let spool = temp_dir("reputation");
        initialize_spool(&spool).unwrap();
        let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
        let mut message = QueuedMessage {
            id: "trace-1".to_string(),
            direction: "inbound".to_string(),
            received_at: "unix:1".to_string(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "sender@example.test".to_string(),
            rcpt_to: vec!["dest@example.test".to_string()],
            status: "incoming".to_string(),
            relay_error: None,
            magika_summary: None,
            magika_decision: None,
            spam_score: 0.0,
            security_score: 0.0,
            reputation_score: 0,
            dnsbl_hits: Vec::new(),
            auth_summary: AuthSummary::default(),
            decision_trace: Vec::new(),
            remote_message_ref: None,
            technical_status: None,
            dsn: None,
            route: None,
            throttle: None,
            data: b"Subject: test\r\n\r\nbody".to_vec(),
        };

        update_reputation(&spool, &config, &message, FilterAction::Accept)
            .await
            .unwrap();
        update_reputation(&spool, &config, &message, FilterAction::Quarantine)
            .await
            .unwrap();
        message.id = "trace-2".to_string();
        update_reputation(&spool, &config, &message, FilterAction::Reject)
            .await
            .unwrap();

        let score = load_reputation_score(
            &spool,
            &config,
            parse_peer_ip(&message.peer),
            &message.mail_from,
        )
        .await
        .unwrap();
        assert_eq!(score, -4);
    }

    #[tokio::test]
    async fn bayespam_learns_tokens_and_scores_spammy_message() {
        let spool = temp_dir("bayespam-train");
        initialize_spool(&spool).unwrap();
        let config = runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());

        train_bayespam(
            &spool,
            &config,
            &training_message("Weekly report", "meeting agenda project status"),
            BayesLabel::Ham,
        )
        .await
        .unwrap();
        train_bayespam(
            &spool,
            &config,
            &training_message("Cheap pills", "cheap pills winner casino bonus pills"),
            BayesLabel::Spam,
        )
        .await
        .unwrap();

        let corpus = load_bayespam_corpus(&spool, &config).await.unwrap();
        assert_eq!(corpus.ham_messages, 1);
        assert_eq!(corpus.spam_messages, 1);
        assert!(corpus.spam_tokens.contains_key("cheap"));

        let score = score_bayespam(
            &spool,
            &config,
            "Cheap pills offer",
            "casino bonus cheap pills now",
            "sender@example.test",
            "mx.example.test",
        )
        .await
        .unwrap()
        .unwrap();

        assert!(score.probability > 0.80);
        assert!(score.contribution > 3.0);
    }

    #[tokio::test]
    async fn outbound_handoff_quarantines_on_bayespam_score() {
        let spool = temp_dir("outbound-bayespam");
        initialize_spool(&spool).unwrap();
        let mut config =
            runtime_config("127.0.0.1:9".to_string(), "http://127.0.0.1:9".to_string());
        config.spam_quarantine_threshold = 4.0;

        train_bayespam(
            &spool,
            &config,
            &training_message("Project update", "meeting notes roadmap delivery"),
            BayesLabel::Ham,
        )
        .await
        .unwrap();
        train_bayespam(
            &spool,
            &config,
            &training_message("Cheap pills", "cheap pills winner casino bonus pills"),
            BayesLabel::Spam,
        )
        .await
        .unwrap();

        let mut request = outbound_request("Cheap pills now");
        request.body_text = "cheap pills winner casino bonus".to_string();
        let response = process_outbound_handoff(&spool, &config, request)
            .await
            .unwrap();

        assert_eq!(response.status, TransportDeliveryStatus::Quarantined);
        assert!(response
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("bayespam score"));
    }

    #[test]
    fn takeri_provider_loads_with_default_command_and_args() {
        let providers = load_antivirus_providers(&["takeri".to_string()]);
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "takeri");
        assert_eq!(
            providers[0].command,
            "/opt/lpe-ct/bin/Shuhari-CyberForge-CLI"
        );
        assert_eq!(
            providers[0].args,
            vec!["takeri".to_string(), "scan".to_string()]
        );
    }

    #[test]
    fn antivirus_output_parser_detects_takeri_infections_and_suspicious_files() {
        let provider = AntivirusProviderConfig {
            id: "takeri".to_string(),
            display_name: "takeri".to_string(),
            command: "/opt/lpe-ct/bin/Shuhari-CyberForge-CLI".to_string(),
            args: vec!["takeri".to_string(), "scan".to_string()],
            infected_markers: vec![
                "status: infected".to_string(),
                "infected files detected".to_string(),
                "infected files:".to_string(),
            ],
            suspicious_markers: vec![
                "status: suspicious".to_string(),
                "suspicious files:".to_string(),
            ],
            clean_markers: vec![
                "status: clean".to_string(),
                "no threats detected".to_string(),
            ],
        };

        let infected = parse_antivirus_output(
            &provider,
            "-------Scan Summary-------\nInfected files: 1\nSuspicious files: 0\n",
            "",
            Some(0),
        )
        .unwrap();
        assert_eq!(infected.decision, AntivirusProviderDecision::Infected);

        let suspicious = parse_antivirus_output(
            &provider,
            "-------Scan Result-------\nStatus: SUSPICIOUS\n",
            "",
            Some(0),
        )
        .unwrap();
        assert_eq!(suspicious.decision, AntivirusProviderDecision::Suspicious);

        let clean =
            parse_antivirus_output(&provider, "No threats detected.\n", "", Some(0)).unwrap();
        assert_eq!(clean.decision, AntivirusProviderDecision::Clean);
    }

    #[test]
    fn auth_summary_uses_structured_outcomes() {
        assert_eq!(summarize_spf(&SpfResult::Pass), "pass");
        assert_eq!(
            summarize_spf(&SpfResult::Fail {
                explanation: Some("policy".to_string())
            }),
            "fail (policy)"
        );
        assert_eq!(
            summarize_dkim(
                &[DkimResult::Pass {
                    domain: "example.test".to_string(),
                    selector: "s1".to_string(),
                    testing: false,
                }],
                true,
            ),
            "pass (aligned)"
        );
        assert_eq!(summarize_dmarc(DmarcDisposition::Reject), "reject");
        assert_eq!(
            spf_disposition(&SpfResult::SoftFail),
            SpfDisposition::SoftFail
        );
        assert_eq!(dkim_disposition(&[DkimResult::None]), DkimDisposition::None);
    }

    #[test]
    fn auth_tempfail_is_detected_for_defer_logic() {
        let assessment = AuthenticationAssessment {
            spf: SpfDisposition::TempError,
            dkim: DkimDisposition::None,
            dkim_aligned: false,
            spf_aligned: false,
            dmarc: DmarcDisposition::None,
            from_domain: "example.test".to_string(),
            spf_domain: "example.test".to_string(),
        };
        assert!(assessment.has_temporary_failure());
    }

    #[test]
    fn auth_score_application_penalizes_failures_and_alignment_gaps() {
        let assessment = AuthenticationAssessment {
            spf: SpfDisposition::Fail,
            dkim: DkimDisposition::PermFail,
            dkim_aligned: false,
            spf_aligned: false,
            dmarc: DmarcDisposition::Quarantine,
            from_domain: "from.example.test".to_string(),
            spf_domain: "bounce.example.test".to_string(),
        };
        let mut spam_score = 0.0;
        let mut security_score = 0.0;
        let mut trace = Vec::new();

        apply_authentication_scores(
            &assessment,
            &mut spam_score,
            &mut security_score,
            &mut trace,
        );

        assert!(spam_score >= 4.5);
        assert!(security_score >= 5.0);
        assert!(trace.iter().any(|entry| entry.stage == "spf-alignment"));
        assert!(trace.iter().any(|entry| entry.stage == "dkim-alignment"));
    }

    #[test]
    fn retry_backoff_grows_with_attempt_count_and_caps() {
        assert_eq!(retry_after_seconds(300, 0), 300);
        assert_eq!(retry_after_seconds(300, 1), 600);
        assert_eq!(retry_after_seconds(300, 3), 2400);
        assert_eq!(retry_after_seconds(300, 9), 3600);
    }

    #[test]
    fn dnsbl_query_name_reverses_ipv4_and_ipv6_addresses() {
        let ipv4: IpAddr = "203.0.113.7".parse().unwrap();
        assert_eq!(
            dnsbl_query_name(ipv4, "zen.spamhaus.org"),
            "7.113.0.203.zen.spamhaus.org"
        );

        let ipv6: IpAddr = "2001:db8::1".parse().unwrap();
        assert!(dnsbl_query_name(ipv6, "dnsbl.example.test").ends_with(".dnsbl.example.test"));
    }

    async fn spawn_dummy_smtp(captured: Arc<Mutex<String>>) -> String {
        spawn_dummy_smtp_with_profile(DummySmtpProfile {
            captured: Some(captured),
            ..DummySmtpProfile::default()
        })
        .await
    }

    #[derive(Clone, Default)]
    struct DummySmtpProfile {
        captured: Option<Arc<Mutex<String>>>,
        rcpt_reply: String,
        final_reply: String,
    }

    async fn spawn_dummy_smtp_with_profile(profile: DummySmtpProfile) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_dummy_smtp(stream, profile).await;
        });
        address.to_string()
    }

    async fn handle_dummy_smtp(stream: TcpStream, profile: DummySmtpProfile) {
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        writer.write_all(b"220 dummy\r\n").await.unwrap();

        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            let trimmed = line.trim_end().to_string();
            if trimmed == "DATA" {
                writer.write_all(b"354 data\r\n").await.unwrap();
                let mut data = String::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).await.unwrap();
                    if line == ".\r\n" {
                        break;
                    }
                    data.push_str(&line);
                }
                if let Some(captured) = &profile.captured {
                    *captured.lock().unwrap() = data;
                }
                let final_reply = if profile.final_reply.is_empty() {
                    "250 stored".to_string()
                } else {
                    profile.final_reply.clone()
                };
                writer
                    .write_all(format!("{final_reply}\r\n").as_bytes())
                    .await
                    .unwrap();
            } else if trimmed == "QUIT" {
                writer.write_all(b"221 bye\r\n").await.unwrap();
                break;
            } else if trimmed.starts_with("RCPT TO:") && !profile.rcpt_reply.is_empty() {
                writer
                    .write_all(format!("{}\r\n", profile.rcpt_reply).as_bytes())
                    .await
                    .unwrap();
            } else {
                writer.write_all(b"250 ok\r\n").await.unwrap();
            }
        }
    }

    fn outbound_request(subject: &str) -> OutboundMessageHandoffRequest {
        OutboundMessageHandoffRequest {
            queue_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            from_address: "sender@example.test".to_string(),
            from_display: Some("Sender".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            to: vec![TransportRecipient {
                address: "dest@example.test".to_string(),
                display_name: Some("Dest".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: subject.to_string(),
            body_text: "Body".to_string(),
            body_html_sanitized: None,
            internet_message_id: Some(format!("<{}@test>", subject.to_ascii_lowercase())),
            attempt_count: 0,
            last_attempt_error: None,
        }
    }

    async fn spawn_dummy_core(captured: Arc<Mutex<Option<InboundDeliveryRequest>>>) -> String {
        async fn accept(
            axum::extract::State(captured): axum::extract::State<
                Arc<Mutex<Option<InboundDeliveryRequest>>>,
            >,
            Json(request): Json<InboundDeliveryRequest>,
        ) -> Json<InboundDeliveryResponse> {
            *captured.lock().unwrap() = Some(request.clone());
            Json(InboundDeliveryResponse {
                accepted: true,
                delivered_mailboxes: request.rcpt_to.clone(),
                detail: None,
            })
        }

        let router = Router::new()
            .route("/internal/lpe-ct/inbound-deliveries", post(accept))
            .with_state(captured);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://{}", address)
    }
}

mod base64_bytes {
    use super::BASE64;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&BASE64.encode(value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        BASE64.decode(encoded).map_err(serde::de::Error::custom)
    }
}
