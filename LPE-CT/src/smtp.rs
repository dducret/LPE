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
use sqlx::{types::Json, PgPool, Row};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet},
    env,
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{BufReader as StdBufReader, Cursor},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    task::{Context as TaskContext, Poll},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf},
    net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream},
    process::Command,
};
use tokio_rustls::{
    rustls::{
        pki_types::{CertificateDer, PrivateKeyDer},
        ServerConfig,
    },
    TlsAcceptor,
};
use tracing::{info, warn};

use crate::{dkim_signing, integration_shared_secret, observability, storage, transport_policy};

mod audit;
use audit::{
    append_transport_audit, postfix_style_mail_log_line, quarantine_search_text,
    TransportAuditEvent,
};
mod delivery_bridge;
use delivery_bridge::deliver_inbound_message;
mod protocol;

use protocol::{
    expect_smtp, read_smtp_data, read_smtp_reply, smtp_command, smtp_command_reply, write_smtp,
};
pub(crate) use protocol::{
    max_smtp_message_size_bytes, parse_smtp_path, smtp_path_error_reply, ParsedSmtpPath,
    SmtpPathError, SmtpPathKind,
};

mod session;
use session::{
    handle_smtp_command, handle_smtp_session, receive_message, receive_message_with_validator,
    SmtpCommandOutcome, SmtpTransaction,
};

const BAYESPAM_MIN_SCORING_TOKENS: usize = 3;
const MAX_SMTP_COMMAND_LINE_LEN: usize = 510;
const MAX_SMTP_RCPT_PER_TRANSACTION: usize = 25;

static SMTP_ACTIVE_SESSIONS: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    pub(crate) primary_upstream: String,
    pub(crate) secondary_upstream: String,
    pub(crate) outbound_ehlo_name: String,
    pub(crate) core_delivery_base_url: String,
    pub(crate) mutual_tls_required: bool,
    pub(crate) fallback_to_hold_queue: bool,
    pub(crate) drain_mode: bool,
    pub(crate) quarantine_enabled: bool,
    pub(crate) greylisting_enabled: bool,
    pub(crate) greylist_delay_seconds: u64,
    pub(crate) antivirus_enabled: bool,
    pub(crate) antivirus_fail_closed: bool,
    pub(crate) antivirus_provider_chain: Vec<String>,
    pub(crate) antivirus_providers: Vec<AntivirusProviderConfig>,
    pub(crate) bayespam_enabled: bool,
    pub(crate) bayespam_auto_learn: bool,
    pub(crate) bayespam_score_weight: f32,
    pub(crate) bayespam_min_token_length: u32,
    pub(crate) bayespam_max_tokens: u32,
    pub(crate) require_spf: bool,
    pub(crate) require_dkim_alignment: bool,
    pub(crate) require_dmarc_enforcement: bool,
    pub(crate) defer_on_auth_tempfail: bool,
    pub(crate) dnsbl_enabled: bool,
    pub(crate) dnsbl_zones: Vec<String>,
    pub(crate) reputation_enabled: bool,
    pub(crate) reputation_quarantine_threshold: i32,
    pub(crate) reputation_reject_threshold: i32,
    pub(crate) spam_quarantine_threshold: f32,
    pub(crate) spam_reject_threshold: f32,
    pub(crate) max_message_size_mb: u32,
    pub(crate) max_concurrent_sessions: u32,
    pub(crate) routing_rules: Vec<OutboundRoutingRule>,
    pub(crate) throttle_enabled: bool,
    pub(crate) throttle_rules: Vec<OutboundThrottleRule>,
    pub(crate) address_policy: transport_policy::AddressPolicyConfig,
    pub(crate) recipient_verification: transport_policy::RecipientVerificationConfig,
    pub(crate) attachment_policy: transport_policy::AttachmentPolicyConfig,
    pub(crate) dkim: dkim_signing::DkimConfig,
    pub(crate) local_db: storage::LocalDbConfig,
    pub(crate) accepted_domains: Vec<AcceptedDomainConfig>,
}

#[derive(Debug, Clone)]
pub(crate) struct AcceptedDomainConfig {
    pub(crate) domain: String,
    pub(crate) rbl_checks: bool,
    pub(crate) spf_checks: bool,
    pub(crate) greylisting: bool,
    pub(crate) accept_null_reverse_path: bool,
    pub(crate) verified: bool,
}

#[derive(Debug, Clone, Copy)]
struct InboundDomainPolicy {
    rbl_checks: bool,
    spf_checks: bool,
    greylisting: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct OutboundRoutingRule {
    id: String,
    sender_domain: Option<String>,
    recipient_domain: Option<String>,
    relay_target: String,
}

#[derive(Debug, Clone)]
pub(crate) struct OutboundThrottleRule {
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
    received_unix: i64,
    peer: String,
    helo: String,
    mail_from: String,
    sender_domain: Option<String>,
    rcpt_to: Vec<String>,
    recipient_domains: Vec<String>,
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
    remote_message_ref: Option<String>,
    route_target: Option<String>,
    search_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QuarantineSummary {
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
    pub spam_score: f32,
    pub security_score: f32,
    pub reputation_score: i32,
    pub dnsbl_hits: Vec<String>,
    pub auth_summary: Value,
    pub magika_summary: Option<String>,
    pub magika_decision: Option<String>,
    pub remote_message_ref: Option<String>,
    pub route_target: Option<String>,
    pub decision_summary: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct QuarantineQuery {
    pub q: Option<String>,
    pub trace_id: Option<String>,
    pub sender: Option<String>,
    pub recipient: Option<String>,
    pub internet_message_id: Option<String>,
    pub route_target: Option<String>,
    pub reason: Option<String>,
    pub direction: Option<String>,
    pub status: Option<String>,
    pub domain: Option<String>,
    pub min_spam_score: Option<f32>,
    pub min_security_score: Option<f32>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TraceAttachmentSummary {
    pub name: String,
    pub size_bytes: u64,
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
    pub dnsbl_hits: Vec<String>,
    pub auth_summary: Value,
    pub magika_summary: Option<String>,
    pub magika_decision: Option<String>,
    pub technical_status: Option<TransportTechnicalStatus>,
    pub dsn: Option<TransportDsnReport>,
    pub route: Option<TransportRouteDecision>,
    pub throttle: Option<TransportThrottleStatus>,
    pub message_size_bytes: u64,
    pub headers: Vec<(String, String)>,
    pub body_excerpt: String,
    pub body_content: String,
    pub attachments: Vec<TraceAttachmentSummary>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ThrottleState {
    hits: Vec<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct AntivirusProviderConfig {
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

const DEFAULT_GREYLIST_DELAY_SECONDS: u64 = 30;
const DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS: u32 = 60;

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

pub(crate) const POLICY_ARTIFACTS: [&str; 6] = [
    "postgres: reputation_entries",
    "postgres: bayespam_corpora",
    "postgres: throttle_windows",
    "postgres: greylist_entries",
    "spool: policy/transport-audit.jsonl",
    "spool: policy/digest-reports/",
];

pub(crate) fn initialize_spool(spool_dir: &Path) -> Result<()> {
    for queue in SPOOL_QUEUES {
        fs::create_dir_all(spool_dir.join(queue))
            .with_context(|| format!("unable to create spool queue {queue}"))?;
    }
    Ok(())
}

pub(crate) async fn prepare_local_store(spool_dir: &Path, config: &RuntimeConfig) -> Result<()> {
    let Some(_pool) = ensure_local_db_schema(config).await? else {
        return Ok(());
    };
    reindex_quarantine_spool(spool_dir, config).await?;
    Ok(())
}

pub(crate) async fn ensure_local_db_schema(
    config: &RuntimeConfig,
) -> Result<Option<&'static PgPool>> {
    storage::ensure_local_db_schema(&config.local_db).await
}

async fn reindex_quarantine_spool(spool_dir: &Path, config: &RuntimeConfig) -> Result<()> {
    let quarantine_dir = spool_dir.join("quarantine");
    if !quarantine_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(quarantine_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let message = load_message_from_path(&path)?;
        persist_quarantine_metadata(spool_dir, config, &message).await?;
    }
    Ok(())
}

pub(crate) fn queue_metrics(
    spool_dir: &Path,
    upstream_reachable: bool,
) -> Result<super::QueueMetrics> {
    let incoming = inspect_queue(spool_dir, "incoming")?;
    let outbound = inspect_queue(spool_dir, "outbound")?;
    let deferred = inspect_queue(spool_dir, "deferred")?;
    let quarantine = inspect_queue(spool_dir, "quarantine")?;
    let held = inspect_queue(spool_dir, "held")?;
    let sent = inspect_queue(spool_dir, "sent")?;
    let bounces = inspect_queue(spool_dir, "bounces")?;
    Ok(super::QueueMetrics {
        inbound_messages: incoming.messages + sent.messages,
        incoming_messages: incoming.messages,
        active_messages: outbound.messages,
        deferred_messages: deferred.messages,
        quarantined_messages: quarantine.messages,
        held_messages: held.messages,
        corrupt_messages: incoming.corrupt
            + outbound.corrupt
            + deferred.corrupt
            + quarantine.corrupt
            + held.corrupt
            + sent.corrupt
            + bounces.corrupt,
        delivery_attempts_last_hour: sent.messages + deferred.messages,
        upstream_reachable,
    })
}

pub(crate) async fn run_smtp_listener(
    bind_address: String,
    dashboard_store: Arc<Mutex<super::DashboardState>>,
    spool_dir: PathBuf,
) -> Result<()> {
    let listener = TcpListener::bind(&bind_address)
        .await
        .with_context(|| format!("unable to bind SMTP listener on {bind_address}"))?;
    info!("lpe-ct smtp listener active on {bind_address}");

    loop {
        let (stream, peer) = listener.accept().await?;
        let max_concurrent_sessions = runtime_config_from_store(&dashboard_store)
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
        let dashboard_store = dashboard_store.clone();
        let spool_dir = spool_dir.clone();
        let starttls = match smtp_starttls_acceptor_from_store(&dashboard_store) {
            Ok(starttls) => starttls,
            Err(error) => {
                warn!(error = %error, "public TLS profile is not usable; STARTTLS will not be advertised for this SMTP session");
                None
            }
        };
        tokio::spawn(async move {
            if let Err(error) =
                handle_smtp_session(stream, peer, dashboard_store, spool_dir, starttls).await
            {
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
        outbound_ehlo_name: sanitize_outbound_ehlo_name(&dashboard.relay.outbound_ehlo_name),
        core_delivery_base_url: dashboard.relay.core_delivery_base_url.clone(),
        mutual_tls_required: dashboard.relay.mutual_tls_required,
        fallback_to_hold_queue: dashboard.relay.fallback_to_hold_queue,
        drain_mode: dashboard.policies.drain_mode,
        quarantine_enabled: dashboard.policies.quarantine_enabled,
        greylisting_enabled: dashboard.policies.greylisting_enabled,
        greylist_delay_seconds: env::var("LPE_CT_GREYLIST_DELAY_SECONDS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_GREYLIST_DELAY_SECONDS)
            .max(1),
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
        address_policy: transport_policy::AddressPolicyConfig {
            allow_senders: dashboard.policies.address_policy.allow_senders.clone(),
            block_senders: dashboard.policies.address_policy.block_senders.clone(),
            allow_recipients: dashboard.policies.address_policy.allow_recipients.clone(),
            block_recipients: dashboard.policies.address_policy.block_recipients.clone(),
        },
        recipient_verification: transport_policy::RecipientVerificationConfig {
            enabled: dashboard.policies.recipient_verification.enabled,
            fail_closed: dashboard.policies.recipient_verification.fail_closed,
            cache_ttl_seconds: u64::from(
                dashboard.policies.recipient_verification.cache_ttl_seconds,
            ),
            local_db: storage::LocalDbConfig {
                enabled: dashboard.local_data_stores.dedicated_postgres.enabled,
                database_url: env::var("LPE_CT_LOCAL_DB_URL")
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            },
        },
        attachment_policy: transport_policy::AttachmentPolicyConfig {
            allow_extensions: dashboard
                .policies
                .attachment_policy
                .allow_extensions
                .clone(),
            block_extensions: dashboard
                .policies
                .attachment_policy
                .block_extensions
                .clone(),
            allow_mime_types: dashboard
                .policies
                .attachment_policy
                .allow_mime_types
                .clone(),
            block_mime_types: dashboard
                .policies
                .attachment_policy
                .block_mime_types
                .clone(),
            allow_detected_types: dashboard
                .policies
                .attachment_policy
                .allow_detected_types
                .clone(),
            block_detected_types: dashboard
                .policies
                .attachment_policy
                .block_detected_types
                .clone(),
        },
        dkim: dkim_signing::DkimConfig {
            enabled: dashboard.policies.dkim.enabled,
            headers: dashboard.policies.dkim.headers.clone(),
            over_sign: dashboard.policies.dkim.over_sign,
            expiration_seconds: dashboard.policies.dkim.expiration_seconds.map(u64::from),
            keys: dashboard
                .policies
                .dkim
                .domains
                .iter()
                .filter(|entry| entry.enabled)
                .map(|entry| dkim_signing::DkimKeyConfig {
                    domain: entry.domain.clone(),
                    selector: entry.selector.clone(),
                    key_path: entry.private_key_path.clone(),
                })
                .collect(),
        },
        local_db: storage::LocalDbConfig {
            enabled: dashboard.local_data_stores.dedicated_postgres.enabled,
            database_url: env::var("LPE_CT_LOCAL_DB_URL")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        },
        accepted_domains: dashboard
            .accepted_domains
            .iter()
            .map(|domain| AcceptedDomainConfig {
                domain: domain.domain.clone(),
                rbl_checks: domain.rbl_checks,
                spf_checks: domain.spf_checks,
                greylisting: domain.greylisting,
                accept_null_reverse_path: domain.accept_null_reverse_path,
                verified: domain.verified,
            })
            .collect(),
    }
}

pub(crate) fn runtime_config_from_store(
    dashboard_store: &Arc<Mutex<super::DashboardState>>,
) -> Result<RuntimeConfig> {
    let dashboard = dashboard_store
        .lock()
        .map_err(|_| anyhow!("dashboard state lock poisoned"))?
        .clone();
    Ok(runtime_config_from_dashboard(&dashboard))
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
    let trace_id = format!("lpe-ct-out-{}", payload.queue_id);
    if let Some((queue, message)) = find_message(spool_dir, &trace_id)? {
        let mut audit_message = message.clone();
        audit_message.decision_trace.push(DecisionTraceEntry {
            stage: "custody-invariant".to_string(),
            outcome: "handoff-replay-suppressed".to_string(),
            detail: format!(
                "duplicate outbound handoff for queue_id {} reused existing {queue} custody",
                payload.queue_id
            ),
        });
        let _ = append_transport_audit(spool_dir, config, &queue, &audit_message).await;
        return Ok(outbound_handoff_response_from_spool(
            &payload, &trace_id, &queue, message,
        ));
    }
    let message_id = payload.message_id;
    let internet_message_id = payload.internet_message_id.clone();
    let route = resolve_outbound_route(config, &payload);
    let dkim = dkim_signing::maybe_sign_outbound_message(
        &config.dkim,
        &payload,
        &compose_rfc822_message(&payload),
    )?;
    let mut message = QueuedMessage {
        id: trace_id,
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
        data: dkim.message,
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
    message.decision_trace.push(DecisionTraceEntry {
        stage: "outbound-dkim".to_string(),
        outcome: if dkim.signed { "signed" } else { "skipped" }.to_string(),
        detail: dkim.detail,
    });
    if let Some(reason) = evaluate_outbound_sender_policy(config, &payload) {
        message.status = "held".to_string();
        message.relay_error = Some(reason.clone());
        message.technical_status = Some(TransportTechnicalStatus {
            phase: "mail-from".to_string(),
            smtp_code: Some(550),
            enhanced_code: Some("5.7.1".to_string()),
            remote_host: route.relay_target.clone(),
            detail: Some(reason.clone()),
        });
        message.dsn = Some(TransportDsnReport {
            action: "failed".to_string(),
            status: "5.7.1".to_string(),
            diagnostic_code: Some(format!("smtp; {reason}")),
            remote_mta: route.relay_target.clone(),
        });
        message.decision_trace.push(DecisionTraceEntry {
            stage: "address-policy".to_string(),
            outcome: "reject".to_string(),
            detail: reason.clone(),
        });
        persist_message(spool_dir, "held", &message).await?;
        let _ = append_transport_audit(spool_dir, config, "held", &message).await;
        observability::record_security_event("outbound_failure");
        return Ok(OutboundMessageHandoffResponse {
            queue_id: payload.queue_id,
            status: TransportDeliveryStatus::Failed,
            trace_id: message.id,
            detail: Some(reason),
            remote_message_ref: None,
            retry: None,
            dsn: message.dsn,
            technical: message.technical_status,
            route: Some(route),
            throttle: None,
        });
    }
    for recipient in payload.envelope_recipients() {
        if let transport_policy::AddressPolicyVerdict::Reject(reason) =
            transport_policy::evaluate_address_policy_with_config(
                &config.address_policy,
                transport_policy::AddressRole::Recipient,
                &recipient,
            )
        {
            message.status = "held".to_string();
            message.relay_error = Some(reason.clone());
            message.technical_status = Some(TransportTechnicalStatus {
                phase: "rcpt-to".to_string(),
                smtp_code: Some(550),
                enhanced_code: Some("5.7.1".to_string()),
                remote_host: route.relay_target.clone(),
                detail: Some(reason.clone()),
            });
            message.dsn = Some(TransportDsnReport {
                action: "failed".to_string(),
                status: "5.7.1".to_string(),
                diagnostic_code: Some(format!("smtp; {reason}")),
                remote_mta: route.relay_target.clone(),
            });
            message.decision_trace.push(DecisionTraceEntry {
                stage: "address-policy".to_string(),
                outcome: "reject".to_string(),
                detail: reason.clone(),
            });
            persist_message(spool_dir, "held", &message).await?;
            let _ = append_transport_audit(spool_dir, config, "held", &message).await;
            observability::record_security_event("outbound_failure");
            return Ok(OutboundMessageHandoffResponse {
                queue_id: payload.queue_id,
                status: TransportDeliveryStatus::Failed,
                trace_id: message.id,
                detail: Some(reason),
                remote_message_ref: None,
                retry: None,
                dsn: message.dsn,
                technical: message.technical_status,
                route: Some(route),
                throttle: None,
            });
        }
    }
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
        let _ = append_transport_audit(spool_dir, config, "quarantine", &message).await;
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
        let _ = append_transport_audit(spool_dir, config, "quarantine", &message).await;
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
        let _ = append_transport_audit(spool_dir, config, "quarantine", &message).await;
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
        let _ = append_transport_audit(spool_dir, config, "deferred", &message).await;
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
    let _ = append_transport_audit(spool_dir, config, destination, &message).await;
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

fn outbound_handoff_response_from_spool(
    payload: &OutboundMessageHandoffRequest,
    trace_id: &str,
    queue: &str,
    message: QueuedMessage,
) -> OutboundMessageHandoffResponse {
    let status = outbound_status_from_spool(queue, &message.status);
    let retry = if status == TransportDeliveryStatus::Deferred {
        Some(retry_advice_from_spooled_message(payload, &message))
    } else {
        None
    };
    OutboundMessageHandoffResponse {
        queue_id: payload.queue_id,
        status,
        trace_id: trace_id.to_string(),
        detail: message.relay_error,
        remote_message_ref: message.remote_message_ref,
        retry,
        dsn: message.dsn,
        technical: message.technical_status,
        route: message.route,
        throttle: message.throttle,
    }
}

fn outbound_status_from_spool(queue: &str, message_status: &str) -> TransportDeliveryStatus {
    match queue {
        "sent" => TransportDeliveryStatus::Relayed,
        "deferred" => TransportDeliveryStatus::Deferred,
        "quarantine" => TransportDeliveryStatus::Quarantined,
        "bounces" => TransportDeliveryStatus::Bounced,
        "held" => TransportDeliveryStatus::Failed,
        "outbound" => TransportDeliveryStatus::Queued,
        _ => match message_status {
            "sent" => TransportDeliveryStatus::Relayed,
            "deferred" => TransportDeliveryStatus::Deferred,
            "quarantined" => TransportDeliveryStatus::Quarantined,
            "bounced" => TransportDeliveryStatus::Bounced,
            "outbound" => TransportDeliveryStatus::Queued,
            _ => TransportDeliveryStatus::Failed,
        },
    }
}

fn retry_advice_from_spooled_message(
    payload: &OutboundMessageHandoffRequest,
    message: &QueuedMessage,
) -> TransportRetryAdvice {
    if let Some(throttle) = &message.throttle {
        return TransportRetryAdvice {
            retry_after_seconds: throttle.retry_after_seconds.max(1),
            policy: "throttle".to_string(),
            reason: Some(format!("{} {}", throttle.scope, throttle.key)),
        };
    }

    TransportRetryAdvice {
        retry_after_seconds: retry_after_seconds(
            DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS,
            payload.attempt_count,
        ),
        policy: "lpe-ct-custody-replay".to_string(),
        reason: message.relay_error.clone(),
    }
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

fn normalized(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
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

struct StartTlsStream {
    stream: TcpStream,
    prefix: Cursor<Vec<u8>>,
}

impl StartTlsStream {
    fn new(stream: TcpStream, buffered: Vec<u8>) -> Self {
        Self {
            stream,
            prefix: Cursor::new(buffered),
        }
    }
}

impl AsyncRead for StartTlsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let prefix_position = self.prefix.position() as usize;
        let prefix_len = self.prefix.get_ref().len();
        if prefix_position < prefix_len {
            let available = &self.prefix.get_ref()[prefix_position..];
            let to_copy = available.len().min(buf.remaining());
            buf.put_slice(&available[..to_copy]);
            self.prefix.set_position((prefix_position + to_copy) as u64);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for StartTlsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
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
        .any(|marker| marker_has_positive_match(output, &marker))
}

fn marker_has_positive_match(output: &str, marker: &str) -> bool {
    let mut search_from = 0;
    while let Some(relative_index) = output[search_from..].find(marker) {
        let marker_start = search_from + relative_index;
        let marker_end = marker_start + marker.len();
        if !marker_match_is_explicitly_negative(output, marker_start, marker_end) {
            return true;
        }
        search_from = marker_end;
    }
    false
}

fn marker_match_is_explicitly_negative(
    output: &str,
    marker_start: usize,
    marker_end: usize,
) -> bool {
    let line_start = output[..marker_start]
        .rfind('\n')
        .map_or(0, |index| index + 1);
    let line_end = output[marker_end..]
        .find('\n')
        .map_or(output.len(), |index| marker_end + index);
    let before_marker = output[line_start..marker_start]
        .trim_end_matches(|ch: char| ch.is_whitespace() || matches!(ch, ':' | '=' | '-' | '>'));
    if before_marker.ends_with("no") || before_marker.ends_with("not") {
        return true;
    }

    let after_marker = output[marker_end..line_end].trim_start_matches(|ch: char| {
        ch.is_whitespace() || matches!(ch, ':' | '=' | '-' | '>' | '"' | '\'')
    });
    !after_marker.is_empty()
        && (after_marker.starts_with('0')
            || after_marker.starts_with("false")
            || after_marker.starts_with("no")
            || after_marker.starts_with("none")
            || after_marker.starts_with("not "))
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
    let defer_reasons = Vec::new();
    let mut reject_reasons = Vec::new();
    let mut quarantine_reasons = Vec::new();
    let domain_policy = inbound_domain_policy(config, rcpt_to);
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
        if config.greylisting_enabled && domain_policy.greylisting {
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
        } else if config.greylisting_enabled {
            decision_trace.push(DecisionTraceEntry {
                stage: "greylisting".to_string(),
                outcome: "skipped".to_string(),
                detail: "greylisting disabled for the accepted recipient domain".to_string(),
            });
        }

        if config.dnsbl_enabled && domain_policy.rbl_checks {
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
        } else if config.dnsbl_enabled {
            decision_trace.push(DecisionTraceEntry {
                stage: "rbl-dns-check".to_string(),
                outcome: "skipped".to_string(),
                detail: "RBL checks disabled for the accepted recipient domain".to_string(),
            });
        }

        if domain_policy.spf_checks {
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
                detail: "SPF/DKIM/DMARC checks disabled for the accepted recipient domain"
                    .to_string(),
            });
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

    let (action, reason) = finalize_policy_decision(
        config,
        auth_assessment.as_ref(),
        spam_score,
        security_score,
        reputation_score,
        &mut decision_trace,
        defer_reasons,
        reject_reasons,
        quarantine_reasons,
    );

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

fn finalize_policy_decision(
    config: &RuntimeConfig,
    auth_assessment: Option<&AuthenticationAssessment>,
    spam_score: f32,
    security_score: f32,
    reputation_score: i32,
    decision_trace: &mut Vec<DecisionTraceEntry>,
    mut defer_reasons: Vec<String>,
    mut reject_reasons: Vec<String>,
    mut quarantine_reasons: Vec<String>,
) -> (FilterAction, Option<String>) {
    if config.defer_on_auth_tempfail
        && auth_assessment.is_some_and(AuthenticationAssessment::has_temporary_failure)
    {
        defer_reasons.push("authentication dependency temporarily failed".to_string());
    }
    if config.require_dmarc_enforcement
        && auth_assessment.is_some_and(|assessment| assessment.dmarc == DmarcDisposition::Reject)
    {
        reject_reasons.push("DMARC policy requested reject".to_string());
    }
    if config.require_dmarc_enforcement
        && auth_assessment
            .is_some_and(|assessment| assessment.dmarc == DmarcDisposition::Quarantine)
    {
        quarantine_reasons.push("DMARC policy requested quarantine".to_string());
    }
    if config.require_spf
        && auth_assessment.is_some_and(|assessment| {
            assessment.spf == SpfDisposition::Fail && !assessment.dkim_aligned
        })
    {
        reject_reasons.push("SPF failed and no aligned DKIM signature passed".to_string());
    }
    if config.require_dkim_alignment
        && auth_assessment.is_some_and(|assessment| !assessment.dkim_aligned)
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
        detail: reason.clone().unwrap_or_else(|| {
            format!(
                "message passed SMTP perimeter policy (spam_score={spam_score:.1}, security_score={security_score:.1})"
            )
        }),
    });

    (action, reason)
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

fn inbound_domain_policy(config: &RuntimeConfig, rcpt_to: &[String]) -> InboundDomainPolicy {
    if config.accepted_domains.is_empty() {
        return InboundDomainPolicy {
            rbl_checks: true,
            spf_checks: true,
            greylisting: true,
        };
    }

    let mut policy = InboundDomainPolicy {
        rbl_checks: false,
        spf_checks: false,
        greylisting: false,
    };
    let mut matched = false;

    for recipient in rcpt_to {
        let Some(domain) = recipient.rsplit_once('@').map(|(_, domain)| domain.trim()) else {
            return InboundDomainPolicy {
                rbl_checks: true,
                spf_checks: true,
                greylisting: true,
            };
        };
        let Some(accepted) = config
            .accepted_domains
            .iter()
            .find(|accepted| accepted.verified && accepted.domain.eq_ignore_ascii_case(domain))
        else {
            return InboundDomainPolicy {
                rbl_checks: true,
                spf_checks: true,
                greylisting: true,
            };
        };

        matched = true;
        policy.rbl_checks |= accepted.rbl_checks;
        policy.spf_checks |= accepted.spf_checks;
        policy.greylisting |= accepted.greylisting;
    }

    if matched {
        policy
    } else {
        InboundDomainPolicy {
            rbl_checks: true,
            spf_checks: true,
            greylisting: true,
        }
    }
}

async fn evaluate_greylisting(
    spool_dir: &Path,
    config: &RuntimeConfig,
    ip: IpAddr,
    mail_from: &str,
    rcpt_to: &[String],
) -> Result<Option<String>> {
    let greylist_delay_seconds = config.greylist_delay_seconds.max(1);
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
                release_after_unix: now + greylist_delay_seconds,
                pass_count: 0,
            })
    } else {
        let path = spool_dir.join("greylist").join(format!("{key}.json"));
        if path.exists() {
            serde_json::from_str::<GreylistEntry>(&fs::read_to_string(&path)?)?
        } else {
            GreylistEntry {
                first_seen_unix: now,
                release_after_unix: now + greylist_delay_seconds,
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
            key, greylist_delay_seconds
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
    min_token_length: usize,
    max_tokens: usize,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();
    for token in [subject, visible_text].into_iter().flat_map(|value| {
        value
            .split(|ch: char| !ch.is_ascii_alphanumeric())
            .map(str::trim)
            .filter(|token| token.len() >= min_token_length)
            .map(|token| token.to_ascii_lowercase())
            .collect::<Vec<_>>()
    }) {
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
    let spam_count = *corpus.spam_tokens.get(token).unwrap_or(&0);
    let ham_count = *corpus.ham_tokens.get(token).unwrap_or(&0);
    if spam_count == 0 && ham_count == 0 {
        return None;
    }
    let spam = (spam_count as f64 + 1.0) / (corpus.spam_messages as f64 + 2.0);
    let ham = (ham_count as f64 + 1.0) / (corpus.ham_messages as f64 + 2.0);
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
    if matched < BAYESPAM_MIN_SCORING_TOKENS {
        return Some(BayesOutcome {
            probability: 0.5,
            matched_tokens: matched,
            contribution: 0.0,
        });
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
    _mail_from: &str,
    _helo: &str,
) -> Result<Option<BayesOutcome>> {
    if !config.bayespam_enabled {
        return Ok(None);
    }
    let corpus = load_bayespam_corpus(spool_dir, config).await?;
    let tokens = tokenize_for_bayespam(
        subject,
        visible_text,
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

fn deferred_smtp_reply(message: &QueuedMessage) -> String {
    format!(
        "451 {} (trace {})",
        deferred_smtp_reason(message),
        message.id
    )
}

fn deferred_smtp_reason(message: &QueuedMessage) -> &'static str {
    if message
        .decision_trace
        .iter()
        .any(|entry| entry.stage == "core-delivery")
    {
        "core final delivery temporarily unavailable"
    } else if message
        .decision_trace
        .iter()
        .any(|entry| entry.stage == "greylisting" && entry.outcome == "defer")
    {
        "message temporarily deferred by greylisting"
    } else if message.decision_trace.iter().any(|entry| {
        entry.stage == "policy-trigger"
            && entry.outcome == "defer"
            && entry.detail.contains("authentication")
    }) {
        "message temporarily deferred by authentication dependency"
    } else {
        "message temporarily deferred by perimeter policy"
    }
}

fn rejected_smtp_reply(message: &QueuedMessage) -> String {
    match message
        .relay_error
        .as_deref()
        .map(sanitize_smtp_reply_detail)
        .filter(|reason| !reason.is_empty())
    {
        Some(reason) => format!(
            "554 message rejected by perimeter policy: {} (trace {})",
            reason, message.id
        ),
        None => format!(
            "554 message rejected by perimeter policy (trace {})",
            message.id
        ),
    }
}

fn sanitize_smtp_reply_detail(detail: &str) -> String {
    let normalized = detail
        .chars()
        .map(|ch| {
            if ch.is_ascii_graphic() || ch == ' ' {
                ch
            } else {
                ' '
            }
        })
        .collect::<String>();
    let compacted = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    const MAX_REPLY_DETAIL_LEN: usize = 180;
    if compacted.len() <= MAX_REPLY_DETAIL_LEN {
        compacted
    } else {
        format!(
            "{}...",
            compacted
                .chars()
                .take(MAX_REPLY_DETAIL_LEN)
                .collect::<String>()
        )
    }
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

    if targets.is_empty() {
        return relay_message_direct_mx(config, message, route, attempt_count).await;
    }

    let mut last_error = None;
    for target in targets {
        match relay_message_to_target(
            &target,
            message,
            route,
            attempt_count,
            &config.outbound_ehlo_name,
        )
        .await
        {
            Ok(execution) => return execution,
            Err(error) => last_error = Some((target, error)),
        }
    }

    let (target, error) =
        last_error.unwrap_or_else(|| ("".to_string(), anyhow!("no SMTP target attempted")));
    let detail = error.to_string();
    let status = if is_permanent_relay_error(&detail) {
        TransportDeliveryStatus::Failed
    } else {
        TransportDeliveryStatus::Deferred
    };
    let retry = if status == TransportDeliveryStatus::Deferred {
        let retry_after = retry_after_seconds(DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS, attempt_count);
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

async fn relay_message_direct_mx(
    config: &RuntimeConfig,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
) -> OutboundExecution {
    let resolver = match SystemDnsResolver::new() {
        Ok(resolver) => resolver,
        Err(error) => {
            return direct_mx_failure(
                route,
                attempt_count,
                format!("unable to initialize DNS resolver for direct MX delivery: {error}"),
                None,
                false,
            );
        }
    };

    let mut recipients_by_domain = BTreeMap::<String, Vec<String>>::new();
    for recipient in &message.rcpt_to {
        let Some(domain) = domain_part(recipient) else {
            return direct_mx_failure(
                route,
                attempt_count,
                format!("recipient address has no domain: {recipient}"),
                None,
                true,
            );
        };
        recipients_by_domain
            .entry(domain)
            .or_default()
            .push(recipient.clone());
    }

    let mut relayed = Vec::new();
    let mut last_execution = None;
    let local_domains = recipients_by_domain
        .keys()
        .filter(|domain| accepted_domain_is_verified(config, domain))
        .cloned()
        .collect::<Vec<_>>();
    for domain in local_domains {
        let Some(recipients) = recipients_by_domain.remove(&domain) else {
            continue;
        };
        let execution = deliver_outbound_to_local_recipients(
            config,
            message,
            route,
            attempt_count,
            &recipients,
        )
        .await;
        if execution.status != TransportDeliveryStatus::Relayed {
            return execution;
        }
        relayed.push(format!("{domain} via local-core"));
        last_execution = Some(execution);
    }

    for (domain, recipients) in recipients_by_domain {
        let targets = match direct_mx_targets(&resolver, &domain).await {
            Ok(targets) => targets,
            Err(error) => {
                let detail = error.to_string();
                return direct_mx_failure(
                    route,
                    attempt_count,
                    detail.clone(),
                    Some(domain),
                    is_permanent_direct_mx_error(&detail),
                );
            }
        };

        let mut last_error = None;
        for target in targets {
            match relay_message_to_target_for_recipients(
                &target,
                message,
                route,
                attempt_count,
                &recipients,
                &config.outbound_ehlo_name,
            )
            .await
            {
                Ok(execution) if execution.status == TransportDeliveryStatus::Relayed => {
                    relayed.push(format!("{domain} via {target}"));
                    last_execution = Some(execution);
                    last_error = None;
                    break;
                }
                Ok(execution) => return execution,
                Err(error) => last_error = Some((target, error)),
            }
        }

        if let Some((target, error)) = last_error {
            return direct_mx_failure(
                route,
                attempt_count,
                error.to_string(),
                Some(format!("{domain} via {target}")),
                false,
            );
        }
    }

    let Some(mut execution) = last_execution else {
        return direct_mx_failure(
            route,
            attempt_count,
            "no outbound recipients available for direct MX delivery".to_string(),
            None,
            true,
        );
    };

    if relayed.len() > 1 {
        let has_local = relayed
            .iter()
            .any(|entry| entry.ends_with(" via local-core"));
        let relay_target = if has_local {
            "mixed-local-direct-mx"
        } else {
            "direct-mx"
        };
        execution.detail = Some(format!(
            "outbound delivery completed for {} recipient domain groups",
            relayed.len()
        ));
        execution.remote_message_ref = Some(relayed.join("; "));
        execution.route = Some(TransportRouteDecision {
            rule_id: route.rule_id.clone(),
            relay_target: Some(relay_target.to_string()),
            queue: "sent".to_string(),
        });
    }
    execution
}

async fn deliver_outbound_to_local_recipients(
    config: &RuntimeConfig,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
    recipients: &[String],
) -> OutboundExecution {
    let mut local_message = message.clone();
    local_message.rcpt_to = recipients.to_vec();
    match deliver_inbound_message(config, &local_message).await {
        Ok(delivery) => OutboundExecution {
            status: TransportDeliveryStatus::Relayed,
            detail: Some(format!(
                "delivered to local accepted domain through core delivery bridge: {} mailbox(es)",
                delivery.delivered_mailboxes.len()
            )),
            remote_message_ref: Some(format!("local-core:{}", message.id)),
            retry: None,
            dsn: None,
            technical: Some(TransportTechnicalStatus {
                phase: "local-delivery".to_string(),
                smtp_code: None,
                enhanced_code: Some("2.0.0".to_string()),
                remote_host: Some(config.core_delivery_base_url.clone()),
                detail: Some("delivered through LPE core final-delivery API".to_string()),
            }),
            route: Some(TransportRouteDecision {
                rule_id: route.rule_id.clone(),
                relay_target: Some("local-core".to_string()),
                queue: "sent".to_string(),
            }),
            throttle: None,
        },
        Err(error) => {
            let detail = format!("local core delivery failed: {error}");
            OutboundExecution {
                status: TransportDeliveryStatus::Deferred,
                detail: Some(detail.clone()),
                remote_message_ref: None,
                retry: Some(TransportRetryAdvice {
                    retry_after_seconds: retry_after_seconds(
                        DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS,
                        attempt_count,
                    ),
                    policy: "local-core-delivery".to_string(),
                    reason: Some(detail.clone()),
                }),
                dsn: Some(TransportDsnReport {
                    action: "delayed".to_string(),
                    status: "4.4.1".to_string(),
                    diagnostic_code: Some(format!("smtp; {detail}")),
                    remote_mta: Some("local-core".to_string()),
                }),
                technical: Some(TransportTechnicalStatus {
                    phase: "local-delivery".to_string(),
                    smtp_code: None,
                    enhanced_code: Some("4.4.1".to_string()),
                    remote_host: Some(config.core_delivery_base_url.clone()),
                    detail: Some(detail.clone()),
                }),
                route: Some(TransportRouteDecision {
                    rule_id: route.rule_id.clone(),
                    relay_target: Some("local-core".to_string()),
                    queue: "deferred".to_string(),
                }),
                throttle: None,
            }
        }
    }
}

async fn direct_mx_targets(resolver: &SystemDnsResolver, domain: &str) -> Result<Vec<String>> {
    match resolver.query_mx(domain).await {
        Ok(mut records) if !records.is_empty() => {
            records.sort_by_key(|record| record.preference);
            let mut targets = Vec::new();
            for record in records {
                let exchange = record.exchange.trim().trim_end_matches('.');
                if exchange.is_empty() || exchange == "." {
                    anyhow::bail!(
                        "recipient domain {domain} publishes a null MX and does not accept mail"
                    );
                }
                targets.push(format!("{exchange}:25"));
            }
            Ok(targets)
        }
        Ok(_) | Err(DnsError::NoRecords) => Ok(vec![format!("{domain}:25")]),
        Err(DnsError::NxDomain) => anyhow::bail!("recipient domain {domain} does not exist"),
        Err(DnsError::TempFail) => {
            anyhow::bail!("temporary DNS failure while resolving MX for {domain}")
        }
    }
}

fn direct_mx_failure(
    route: &TransportRouteDecision,
    attempt_count: u32,
    detail: String,
    remote_host: Option<String>,
    permanent: bool,
) -> OutboundExecution {
    let status = if permanent {
        TransportDeliveryStatus::Bounced
    } else {
        TransportDeliveryStatus::Deferred
    };
    OutboundExecution {
        status: status.clone(),
        detail: Some(detail.clone()),
        remote_message_ref: None,
        retry: if status == TransportDeliveryStatus::Deferred {
            Some(TransportRetryAdvice {
                retry_after_seconds: retry_after_seconds(
                    DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS,
                    attempt_count,
                ),
                policy: "direct-mx".to_string(),
                reason: Some(detail.clone()),
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
            status: if status == TransportDeliveryStatus::Bounced {
                "5.1.2".to_string()
            } else {
                "4.4.1".to_string()
            },
            diagnostic_code: Some(format!("smtp; {detail}")),
            remote_mta: remote_host.clone(),
        }),
        technical: Some(TransportTechnicalStatus {
            phase: "mx-lookup".to_string(),
            smtp_code: None,
            enhanced_code: None,
            remote_host,
            detail: Some(detail),
        }),
        route: Some(TransportRouteDecision {
            rule_id: route.rule_id.clone(),
            relay_target: Some("direct-mx".to_string()),
            queue: default_queue_for_status(&status).to_string(),
        }),
        throttle: None,
    }
}

fn is_permanent_direct_mx_error(detail: &str) -> bool {
    let lower = detail.to_ascii_lowercase();
    lower.contains("does not exist")
        || lower.contains("null mx")
        || lower.contains("does not accept mail")
        || lower.contains("recipient address has no domain")
        || lower.contains("no outbound recipients")
}

fn sanitize_outbound_ehlo_name(value: &str) -> String {
    let normalized = value.trim().trim_end_matches('.').to_ascii_lowercase();
    if is_valid_ehlo_hostname(&normalized) {
        normalized
    } else {
        "lpe-ct.local".to_string()
    }
}

fn is_valid_ehlo_hostname(value: &str) -> bool {
    if value.is_empty() || value.len() > 253 || !value.contains('.') {
        return false;
    }
    value.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    })
}

async fn relay_message_to_target(
    target: &str,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
    ehlo_name: &str,
) -> Result<OutboundExecution> {
    relay_message_to_target_for_recipients(
        target,
        message,
        route,
        attempt_count,
        &message.rcpt_to,
        ehlo_name,
    )
    .await
}

async fn relay_message_to_target_for_recipients(
    target: &str,
    message: &QueuedMessage,
    route: &TransportRouteDecision,
    attempt_count: u32,
    recipients: &[String],
    ehlo_name: &str,
) -> Result<OutboundExecution> {
    let address = normalize_smtp_target(target);
    let stream = TcpStream::connect(&address)
        .await
        .with_context(|| format!("unable to connect to relay target {address}"))?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    expect_smtp(&mut reader, 220).await?;
    smtp_command(
        &mut reader,
        &mut writer,
        &format!("EHLO {}", sanitize_outbound_ehlo_name(ehlo_name)),
        250,
    )
    .await?;
    smtp_command(
        &mut reader,
        &mut writer,
        &format!("MAIL FROM:<{}>", message.mail_from),
        250,
    )
    .await?;
    for recipient in recipients {
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
                    let retry_after =
                        retry_after_seconds(DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS, attempt_count);
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
                let retry_after =
                    retry_after_seconds(DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS, attempt_count);
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
                let retry_after =
                    retry_after_seconds(DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS, attempt_count);
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

fn smtp_starttls_acceptor_from_store(
    dashboard_store: &Arc<Mutex<super::DashboardState>>,
) -> Result<Option<TlsAcceptor>> {
    let (cert_path, key_path) = {
        let snapshot = dashboard_store
            .lock()
            .map_err(|_| anyhow!("dashboard state lock poisoned"))?;
        public_tls_paths_from_dashboard(&snapshot)
    };
    smtp_starttls_acceptor_for_paths(cert_path, key_path)
}

fn public_tls_paths_from_dashboard(
    dashboard: &super::DashboardState,
) -> (Option<String>, Option<String>) {
    let Some(active_id) = dashboard
        .network
        .public_tls
        .active_profile_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return (None, None);
    };
    let Some(profile) = dashboard
        .network
        .public_tls
        .profiles
        .iter()
        .find(|profile| profile.id == active_id)
    else {
        return (None, None);
    };
    (
        Some(profile.cert_path.trim().to_string()).filter(|value| !value.is_empty()),
        Some(profile.key_path.trim().to_string()).filter(|value| !value.is_empty()),
    )
}

fn smtp_starttls_acceptor_for_paths(
    cert_path: Option<String>,
    key_path: Option<String>,
) -> Result<Option<TlsAcceptor>> {
    match (cert_path, key_path) {
        (Some(cert_path), Some(key_path)) => {
            let certificates = load_certificates(&cert_path)?;
            let key = load_private_key(&key_path)?;
            let config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certificates, key)?;
            Ok(Some(TlsAcceptor::from(Arc::new(config))))
        }
        (None, None) => Ok(None),
        (Some(_), None) => Err(anyhow!(
            "LPE_CT_PUBLIC_TLS_KEY_PATH must be set when LPE_CT_PUBLIC_TLS_CERT_PATH is set"
        )),
        (None, Some(_)) => Err(anyhow!(
            "LPE_CT_PUBLIC_TLS_CERT_PATH must be set when LPE_CT_PUBLIC_TLS_KEY_PATH is set"
        )),
    }
}

fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = StdBufReader::new(
        File::open(path).with_context(|| format!("unable to open certificate {path}"))?,
    );
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse certificate {path}: {error}"))
        .and_then(|certificates| {
            if certificates.is_empty() {
                anyhow::bail!("no certificate found in {path}");
            }
            Ok(certificates)
        })
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader =
        StdBufReader::new(File::open(path).with_context(|| format!("unable to open key {path}"))?);
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse private key {path}: {error}"))?;
    if let Some(key) = keys.pop() {
        return Ok(PrivateKeyDer::Pkcs8(key));
    }

    let mut reader = StdBufReader::new(
        File::open(path).with_context(|| format!("unable to reopen key {path}"))?,
    );
    let mut keys = rustls_pemfile::rsa_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| anyhow!("unable to parse rsa private key {path}: {error}"))?;
    let Some(key) = keys.pop() else {
        anyhow::bail!("no private key found in {path}");
    };
    Ok(PrivateKeyDer::Pkcs1(key))
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

fn quarantine_summary_from_message(message: &QueuedMessage) -> QuarantineSummary {
    QuarantineSummary {
        trace_id: message.id.clone(),
        queue: "quarantine".to_string(),
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
        spam_score: message.spam_score,
        security_score: message.security_score,
        reputation_score: message.reputation_score,
        dnsbl_hits: message.dnsbl_hits.clone(),
        auth_summary: serde_json::to_value(&message.auth_summary).unwrap_or(Value::Null),
        magika_summary: message.magika_summary.clone(),
        magika_decision: message.magika_decision.clone(),
        remote_message_ref: message.remote_message_ref.clone(),
        route_target: message
            .route
            .as_ref()
            .and_then(|route| route.relay_target.clone()),
        decision_summary: latest_decision_summary(&message.decision_trace),
    }
}

fn latest_decision_summary(trace: &[DecisionTraceEntry]) -> Option<String> {
    trace
        .last()
        .map(|entry| format!("{}:{}", entry.stage, entry.outcome))
}

fn quarantine_matches(item: &QuarantineSummary, query: &QuarantineQuery) -> bool {
    if let Some(trace_id) = normalized(query.trace_id.as_deref()) {
        if item.trace_id != trace_id {
            return false;
        }
    }
    if let Some(sender) = normalized(query.sender.as_deref()) {
        if !item.mail_from.to_ascii_lowercase().contains(&sender) {
            return false;
        }
    }
    if let Some(recipient) = normalized(query.recipient.as_deref()) {
        if !item
            .rcpt_to
            .iter()
            .any(|value| value.to_ascii_lowercase().contains(&recipient))
        {
            return false;
        }
    }
    if let Some(internet_message_id) = normalized(query.internet_message_id.as_deref()) {
        if !item
            .internet_message_id
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase()
            .contains(&internet_message_id)
        {
            return false;
        }
    }
    if let Some(route_target) = normalized(query.route_target.as_deref()) {
        if !item
            .route_target
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase()
            .contains(&route_target)
        {
            return false;
        }
    }
    if let Some(reason) = normalized(query.reason.as_deref()) {
        if !item
            .reason
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase()
            .contains(&reason)
        {
            return false;
        }
    }
    if let Some(direction) = query.direction.as_deref() {
        if item.direction != direction {
            return false;
        }
    }
    if let Some(status) = query.status.as_deref() {
        if item.status != status {
            return false;
        }
    }
    if let Some(min_spam_score) = query.min_spam_score {
        if item.spam_score < min_spam_score {
            return false;
        }
    }
    if let Some(min_security_score) = query.min_security_score {
        if item.security_score < min_security_score {
            return false;
        }
    }
    if let Some(domain) = normalized(query.domain.as_deref()) {
        let sender_matches = domain_part(&item.mail_from).is_some_and(|value| value == domain);
        let recipient_matches = item
            .rcpt_to
            .iter()
            .filter_map(|value| domain_part(value))
            .any(|value| value == domain);
        if !sender_matches && !recipient_matches {
            return false;
        }
    }
    if let Some(q) = normalized(query.q.as_deref()) {
        let haystack = [
            item.trace_id.as_str(),
            item.subject.as_str(),
            item.mail_from.as_str(),
            item.peer.as_str(),
            item.helo.as_str(),
            item.reason.as_deref().unwrap_or(""),
            item.internet_message_id.as_deref().unwrap_or(""),
            item.route_target.as_deref().unwrap_or(""),
            item.decision_summary.as_deref().unwrap_or(""),
        ]
        .into_iter()
        .chain(item.rcpt_to.iter().map(String::as_str))
        .chain(item.dnsbl_hits.iter().map(String::as_str))
        .map(|value| value.to_ascii_lowercase())
        .collect::<Vec<_>>();
        if !haystack.iter().any(|value| value.contains(&q)) {
            return false;
        }
    }
    true
}

fn inspect_headers(data: &[u8]) -> Vec<(String, String)> {
    let mut headers = Vec::new();
    for line in String::from_utf8_lossy(data).lines() {
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            let lower = name.trim().to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "from" | "to" | "cc" | "subject" | "date" | "message-id" | "received"
            ) {
                headers.push((name.trim().to_string(), value.trim().to_string()));
            }
        }
        if headers.len() >= 12 {
            break;
        }
    }
    headers
}

fn body_excerpt(data: &[u8]) -> String {
    let raw = String::from_utf8_lossy(data);
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, value)| value)
        .or_else(|| raw.split_once("\n\n").map(|(_, value)| value))
        .unwrap_or("");
    body.chars()
        .filter(|value| !value.is_control() || matches!(value, '\n' | '\r' | '\t'))
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(280)
        .collect()
}

fn body_content(data: &[u8]) -> String {
    extract_visible_text(data)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| body_excerpt(data))
}

fn attachment_summaries(data: &[u8]) -> Vec<TraceAttachmentSummary> {
    collect_mime_attachment_parts(data)
        .map(|attachments| {
            attachments
                .into_iter()
                .enumerate()
                .map(|(index, attachment)| TraceAttachmentSummary {
                    name: attachment
                        .filename
                        .filter(|value| !value.trim().is_empty())
                        .unwrap_or_else(|| format!("attachment-{}", index + 1)),
                    size_bytes: attachment.bytes.len() as u64,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn spool_path(spool_dir: &Path, queue: &str, id: &str) -> PathBuf {
    spool_dir.join(queue).join(format!("{id}.json"))
}

struct QueueInspection {
    messages: u32,
    corrupt: u32,
}

fn inspect_queue(spool_dir: &Path, queue: &str) -> Result<QueueInspection> {
    let path = spool_dir.join(queue);
    if !path.exists() {
        return Ok(QueueInspection {
            messages: 0,
            corrupt: 0,
        });
    }

    let mut inspection = QueueInspection {
        messages: 0,
        corrupt: 0,
    };
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        inspection.messages += 1;
        if fs::read_to_string(&path)
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .is_none()
        {
            inspection.corrupt += 1;
        }
    }

    Ok(inspection)
}

pub(crate) async fn list_quarantine_items(
    spool_dir: &Path,
    config: &RuntimeConfig,
    query: QuarantineQuery,
) -> Result<Vec<QuarantineSummary>> {
    if let Some(items) = list_quarantine_items_from_db(config, &query).await? {
        return Ok(items);
    }
    list_quarantine_items_from_spool(spool_dir, query)
}

pub(crate) fn list_quarantine_items_from_spool(
    spool_dir: &Path,
    query: QuarantineQuery,
) -> Result<Vec<QuarantineSummary>> {
    let mut items = Vec::new();
    for entry in fs::read_dir(spool_dir.join("quarantine"))? {
        let entry = entry?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let message = load_message_from_path(&entry.path())?;
        items.push(quarantine_summary_from_message(&message));
    }
    items.sort_by(|left, right| right.received_at.cmp(&left.received_at));
    items.retain(|item| quarantine_matches(item, &query));
    items.truncate(query.limit.unwrap_or(50).clamp(1, 200));
    Ok(items)
}

async fn list_quarantine_items_from_db(
    config: &RuntimeConfig,
    query: &QuarantineQuery,
) -> Result<Option<Vec<QuarantineSummary>>> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(None);
    };

    let limit = query.limit.unwrap_or(50).clamp(1, 200) as i64;
    let direction = normalized(query.direction.as_deref());
    let status = normalized(query.status.as_deref());
    let domain = normalized(query.domain.as_deref());
    let trace_id = normalized(query.trace_id.as_deref());
    let sender = normalized(query.sender.as_deref()).map(|value| format!("%{value}%"));
    let recipient = normalized(query.recipient.as_deref()).map(|value| format!("%{value}%"));
    let internet_message_id =
        normalized(query.internet_message_id.as_deref()).map(|value| format!("%{value}%"));
    let route_target = normalized(query.route_target.as_deref()).map(|value| format!("%{value}%"));
    let reason = normalized(query.reason.as_deref()).map(|value| format!("%{value}%"));
    let search_term = normalized(query.q.as_deref());
    let search_pattern = search_term.as_ref().map(|value| format!("%{value}%"));

    let rows = sqlx::query(
        r#"
        SELECT trace_id, direction, status, received_at, peer, helo, mail_from, rcpt_to,
               subject, internet_message_id, reason, spam_score, security_score,
               reputation_score, dnsbl_hits, auth_summary, magika_summary,
               magika_decision, remote_message_ref, route_target, decision_trace
          FROM quarantine_messages
         WHERE ($1::TEXT IS NULL OR LOWER(direction) = $1)
           AND ($2::TEXT IS NULL OR LOWER(status) = $2)
           AND ($3::TEXT IS NULL OR LOWER(trace_id) = $3)
           AND (
                $4::TEXT IS NULL
                OR SPLIT_PART(LOWER(mail_from), '@', 2) = $4
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(recipient_domains) AS recipient_domain(value)
                     WHERE LOWER(recipient_domain.value) = $4
                )
           )
           AND ($5::TEXT IS NULL OR LOWER(mail_from) LIKE $5)
           AND (
                $6::TEXT IS NULL
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(rcpt_to) AS recipient_value(value)
                     WHERE LOWER(recipient_value.value) LIKE $6
                )
           )
           AND ($7::TEXT IS NULL OR LOWER(COALESCE(internet_message_id, '')) LIKE $7)
           AND ($8::TEXT IS NULL OR LOWER(COALESCE(route_target, '')) LIKE $8)
           AND ($9::TEXT IS NULL OR LOWER(COALESCE(reason, '')) LIKE $9)
           AND ($10::REAL IS NULL OR spam_score >= $10)
           AND ($11::REAL IS NULL OR security_score >= $11)
           AND (
                $12::TEXT IS NULL
                OR search_text LIKE $12
                OR to_tsvector('simple', search_text) @@ websearch_to_tsquery('simple', $13)
           )
         ORDER BY received_unix DESC, updated_at DESC
         LIMIT $14
        "#,
    )
    .bind(direction)
    .bind(status)
    .bind(trace_id)
    .bind(domain)
    .bind(sender)
    .bind(recipient)
    .bind(internet_message_id)
    .bind(route_target)
    .bind(reason)
    .bind(query.min_spam_score)
    .bind(query.min_security_score)
    .bind(search_pattern)
    .bind(search_term)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let items = rows
        .into_iter()
        .map(|row| {
            Ok::<QuarantineSummary, anyhow::Error>(QuarantineSummary {
                trace_id: row.try_get("trace_id")?,
                queue: "quarantine".to_string(),
                direction: row.try_get("direction")?,
                status: row.try_get("status")?,
                received_at: row.try_get("received_at")?,
                peer: row.try_get("peer")?,
                helo: row.try_get("helo")?,
                mail_from: row.try_get("mail_from")?,
                rcpt_to: row.try_get::<Json<Vec<String>>, _>("rcpt_to")?.0,
                subject: row.try_get("subject")?,
                internet_message_id: row.try_get("internet_message_id")?,
                reason: row.try_get("reason")?,
                spam_score: row.try_get("spam_score")?,
                security_score: row.try_get("security_score")?,
                reputation_score: row.try_get("reputation_score")?,
                dnsbl_hits: row.try_get::<Json<Vec<String>>, _>("dnsbl_hits")?.0,
                auth_summary: row.try_get::<Json<Value>, _>("auth_summary")?.0,
                magika_summary: row.try_get("magika_summary")?,
                magika_decision: row.try_get("magika_decision")?,
                remote_message_ref: row.try_get("remote_message_ref")?,
                route_target: row.try_get("route_target")?,
                decision_summary: row
                    .try_get::<Json<Vec<Value>>, _>("decision_trace")?
                    .0
                    .last()
                    .and_then(|value| {
                        let stage = value.get("stage")?.as_str()?;
                        let outcome = value.get("outcome")?.as_str()?;
                        Some(format!("{stage}:{outcome}"))
                    }),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(items))
}

pub(crate) fn load_trace_details(spool_dir: &Path, trace_id: &str) -> Result<Option<TraceDetails>> {
    let Some((queue, message)) = find_message(spool_dir, trace_id)? else {
        return Ok(None);
    };
    Ok(Some(trace_details_from_message(&queue, &message)))
}

pub(crate) async fn retry_trace(
    spool_dir: &Path,
    config: &RuntimeConfig,
    trace_id: &str,
) -> Result<Option<TraceActionResult>> {
    transition_trace(spool_dir, config, trace_id, TraceAction::Retry).await
}

pub(crate) async fn release_trace(
    spool_dir: &Path,
    config: &RuntimeConfig,
    trace_id: &str,
) -> Result<Option<TraceActionResult>> {
    transition_trace(spool_dir, config, trace_id, TraceAction::Release).await
}

pub(crate) async fn delete_trace(
    spool_dir: &Path,
    config: &RuntimeConfig,
    trace_id: &str,
) -> Result<Option<TraceActionResult>> {
    transition_trace(spool_dir, config, trace_id, TraceAction::Delete).await
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
        dnsbl_hits: message.dnsbl_hits.clone(),
        auth_summary: serde_json::to_value(&message.auth_summary).unwrap_or(Value::Null),
        magika_summary: message.magika_summary.clone(),
        magika_decision: message.magika_decision.clone(),
        technical_status: message.technical_status.clone(),
        dsn: message.dsn.clone(),
        route: message.route.clone(),
        throttle: message.throttle.clone(),
        message_size_bytes: message.data.len() as u64,
        headers: inspect_headers(&message.data),
        body_excerpt: body_excerpt(&message.data),
        body_content: body_content(&message.data),
        attachments: attachment_summaries(&message.data),
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
    Delete,
}

async fn transition_trace(
    spool_dir: &Path,
    config: &RuntimeConfig,
    trace_id: &str,
    action: TraceAction,
) -> Result<Option<TraceActionResult>> {
    let Some((queue, mut message)) = find_message(spool_dir, trace_id)? else {
        return Ok(None);
    };
    if matches!(action, TraceAction::Delete) && !trace_queue_can_be_deleted(&queue) {
        return Ok(Some(TraceActionResult {
            trace_id: message.id.clone(),
            from_queue: queue,
            to_queue: String::new(),
            status: message.status.clone(),
            detail: "only active queue custody traces can be deleted".to_string(),
        }));
    }
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
    if !matches!(action, TraceAction::Delete) {
        message.remote_message_ref = None;
        message.technical_status = None;
        message.dsn = None;
        message.route = None;
        message.throttle = None;
    }
    message.decision_trace.push(DecisionTraceEntry {
        stage: "operator-action".to_string(),
        outcome: match action {
            TraceAction::Retry => "retry".to_string(),
            TraceAction::Release => "release".to_string(),
            TraceAction::Delete => "delete".to_string(),
        },
        detail: if matches!(action, TraceAction::Delete) {
            format!("operator deleted trace from {queue}")
        } else {
            format!("operator moved trace into {target_queue}")
        },
    });
    if matches!(action, TraceAction::Delete) {
        message.status = "deleted".to_string();
        let _ = append_transport_audit(spool_dir, config, "deleted", &message).await;
        tokio::fs::remove_file(spool_path(spool_dir, &queue, &message.id)).await?;
    } else {
        move_message(spool_dir, &message, &queue, target_queue).await?;
        let _ = append_transport_audit(spool_dir, config, target_queue, &message).await;
    }
    if queue == "quarantine" {
        remove_quarantine_metadata_or_warn(config, &message.id).await;
    }
    Ok(Some(TraceActionResult {
        trace_id: message.id.clone(),
        from_queue: queue.clone(),
        to_queue: target_queue.to_string(),
        status: message.status.clone(),
        detail: if matches!(action, TraceAction::Delete) {
            format!("trace deleted from {}", queue)
        } else {
            format!("trace moved into {target_queue}")
        },
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
        ("quarantine", _, TraceAction::Delete) => Some("deleted"),
        ("incoming", _, TraceAction::Delete) => Some("incoming"),
        ("outbound", _, TraceAction::Delete) => Some("outbound"),
        ("deferred", _, TraceAction::Delete) => Some("deferred"),
        ("held", _, TraceAction::Delete) => Some("held"),
        ("bounces", _, TraceAction::Delete) => Some("bounces"),
        _ => None,
    }
}

fn trace_queue_can_be_deleted(queue: &str) -> bool {
    matches!(
        queue,
        "incoming" | "outbound" | "deferred" | "held" | "quarantine" | "bounces"
    )
}

fn evaluate_outbound_sender_policy(
    config: &RuntimeConfig,
    payload: &OutboundMessageHandoffRequest,
) -> Option<String> {
    for address in outbound_sender_policy_addresses(payload) {
        if let transport_policy::AddressPolicyVerdict::Reject(reason) =
            transport_policy::evaluate_address_policy_with_config(
                &config.address_policy,
                transport_policy::AddressRole::Sender,
                address,
            )
        {
            return Some(reason);
        }
    }
    None
}

fn outbound_sender_policy_addresses<'a>(
    payload: &'a OutboundMessageHandoffRequest,
) -> Vec<&'a str> {
    let mut addresses = vec![payload.from_address.as_str()];
    if let Some(sender) = payload
        .sender_address
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case(&payload.from_address))
    {
        addresses.push(sender);
    }
    addresses
}

fn quarantine_metadata(spool_dir: &Path, message: &QueuedMessage) -> QuarantineMetadata {
    let sender_domain = domain_part(&message.mail_from);
    let recipient_domains = message
        .rcpt_to
        .iter()
        .filter_map(|value| domain_part(value))
        .collect::<Vec<_>>();
    QuarantineMetadata {
        trace_id: message.id.clone(),
        direction: message.direction.clone(),
        status: message.status.clone(),
        received_at: message.received_at.clone(),
        received_unix: parse_unix_timestamp(&message.received_at).unwrap_or(0) as i64,
        peer: message.peer.clone(),
        helo: message.helo.clone(),
        mail_from: message.mail_from.clone(),
        sender_domain,
        rcpt_to: message.rcpt_to.clone(),
        recipient_domains,
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
        remote_message_ref: message.remote_message_ref.clone(),
        route_target: message
            .route
            .as_ref()
            .and_then(|route| route.relay_target.clone()),
        search_text: quarantine_search_text(message),
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
            trace_id, direction, status, received_at, received_unix, peer, helo, mail_from,
            sender_domain, rcpt_to, recipient_domains, subject, internet_message_id, spool_path,
            reason, spam_score, security_score, reputation_score, dnsbl_hits, auth_summary,
            decision_trace, magika_summary, magika_decision, remote_message_ref, route_target,
            search_text
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20, $21, $22,
            $23, $24, $25, $26
        )
        ON CONFLICT (trace_id) DO UPDATE SET
            status = EXCLUDED.status,
            received_at = EXCLUDED.received_at,
            received_unix = EXCLUDED.received_unix,
            peer = EXCLUDED.peer,
            helo = EXCLUDED.helo,
            mail_from = EXCLUDED.mail_from,
            sender_domain = EXCLUDED.sender_domain,
            rcpt_to = EXCLUDED.rcpt_to,
            recipient_domains = EXCLUDED.recipient_domains,
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
            remote_message_ref = EXCLUDED.remote_message_ref,
            route_target = EXCLUDED.route_target,
            search_text = EXCLUDED.search_text,
            updated_at = NOW()
        "#,
    )
    .bind(&metadata.trace_id)
    .bind(&metadata.direction)
    .bind(&metadata.status)
    .bind(&metadata.received_at)
    .bind(metadata.received_unix)
    .bind(&metadata.peer)
    .bind(&metadata.helo)
    .bind(&metadata.mail_from)
    .bind(&metadata.sender_domain)
    .bind(Json(metadata.rcpt_to))
    .bind(Json(metadata.recipient_domains))
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
    .bind(&metadata.remote_message_ref)
    .bind(&metadata.route_target)
    .bind(&metadata.search_text)
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

async fn remove_quarantine_metadata(config: &RuntimeConfig, trace_id: &str) -> Result<()> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(());
    };
    sqlx::query("DELETE FROM quarantine_messages WHERE trace_id = $1")
        .bind(trace_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn remove_quarantine_metadata_or_warn(config: &RuntimeConfig, trace_id: &str) {
    if let Err(error) = remove_quarantine_metadata(config, trace_id).await {
        warn!(trace_id = trace_id, error = %error, "unable to remove quarantine metadata");
    }
}

fn recipient_domain_is_accepted(config: &RuntimeConfig, recipient: &str) -> bool {
    if config.accepted_domains.is_empty() {
        return false;
    }
    let Some(domain) = domain_part(recipient) else {
        return false;
    };
    accepted_domain_is_verified(config, &domain)
}

fn accepted_domain_is_verified(config: &RuntimeConfig, domain: &str) -> bool {
    config
        .accepted_domains
        .iter()
        .any(|accepted| accepted.verified && accepted.domain.eq_ignore_ascii_case(&domain))
}

fn recipient_domain_accepts_null_reverse_path(config: &RuntimeConfig, recipient: &str) -> bool {
    if config.accepted_domains.is_empty() {
        return false;
    }
    let Some(domain) = domain_part(recipient) else {
        return false;
    };
    config.accepted_domains.iter().any(|accepted| {
        accepted.verified
            && accepted.accept_null_reverse_path
            && accepted.domain.eq_ignore_ascii_case(&domain)
    })
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
    if let Some(sender_address) = payload
        .sender_address
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case(&payload.from_address))
    {
        lines.push(format!(
            "Sender: {}",
            format_address(sender_address, payload.sender_display.as_deref())
        ));
    }
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
    lower.contains("mutual tls relay is configured but not implemented")
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

fn parse_unix_timestamp(value: &str) -> Option<u64> {
    value.strip_prefix("unix:")?.parse::<u64>().ok()
}

#[cfg(test)]
mod tests;

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
