use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use email_auth::{
    common::dns::{DnsError, DnsResolver},
    dmarc::Disposition as DmarcDisposition,
};
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
    env, fs,
    hash::{Hash, Hasher},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream},
    process::Command,
};
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

use crate::{dkim_signing, integration_shared_secret, observability, storage, transport_policy};

mod anti_abuse;
mod antivirus;
mod audit;
mod auth;
mod bayes;
mod inbound_policy;
use anti_abuse::{
    dnsbl_query_name, evaluate_greylisting, query_dnsbl, DnsblOutcome, GreylistEntry,
};
use antivirus::{
    classify_inbound_message, evaluate_antivirus_policy, load_antivirus_providers,
    parse_antivirus_output, AntivirusProviderConfig, AntivirusProviderDecision,
    InboundMagikaOutcome,
};
use audit::{
    append_transport_audit, postfix_style_mail_log_line, quarantine_search_text,
    TransportAuditEvent,
};
use auth::{
    apply_authentication_scores, authenticate_message, dkim_disposition, spf_disposition,
    summarize_dkim, summarize_dmarc, summarize_spf, AuthSummary, AuthenticationAssessment,
    DkimDisposition, SpfDisposition,
};
use bayes::train_bayespam;
pub(crate) use bayes::{
    load_bayespam_corpus, score_bayespam, BayesLabel, BAYESPAM_MIN_SCORING_TOKENS,
};
use inbound_policy::{apply_filter_verdict, evaluate_inbound_policy, finalize_policy_decision};
mod delivery_bridge;
use delivery_bridge::deliver_inbound_message;
mod dns;
use dns::SystemDnsResolver;
mod dsn;
use dsn::{
    deferred_smtp_reply, direct_mx_failure, is_permanent_direct_mx_error, is_permanent_relay_error,
    parse_enhanced_status, rejected_smtp_reply,
};
mod policy;
mod quarantine;
mod queue_store;
mod reputation;
use policy::{
    accepted_domain_is_verified, domain_part, inbound_domain_policy, matches_any_domain,
    matches_domain, normalized, recipient_domain_accepts_null_reverse_path,
    recipient_domain_is_accepted,
};
pub(crate) use quarantine::{list_quarantine_items, list_quarantine_items_from_spool};
use quarantine::{
    persist_quarantine_metadata, persist_quarantine_metadata_or_warn,
    remove_quarantine_metadata_or_warn,
};
use queue_store::{
    find_message, inspect_queue, load_message_from_path, move_message, persist_message, spool_path,
};
mod protocol;

use protocol::{
    expect_smtp, read_smtp_data, read_smtp_reply, smtp_command, smtp_command_reply, write_smtp,
};
pub(crate) use protocol::{
    max_smtp_message_size_bytes, parse_smtp_path, smtp_path_error_reply, ParsedSmtpPath,
    SmtpPathError, SmtpPathKind,
};
use reputation::{load_reputation_score, update_reputation};

mod outbound;
mod outbound_delivery;
mod outbound_policy;
pub(crate) use outbound::{compose_rfc822_message, encode_quoted_printable};
use outbound_delivery::{relay_message, sanitize_outbound_ehlo_name};
use outbound_policy::{
    default_queue_for_status, evaluate_outbound_throttle, outbound_handoff_response_from_spool,
    resolve_outbound_route,
};

mod session;
use session::{
    handle_smtp_command, handle_smtp_session, receive_message, receive_message_with_validator,
    SmtpCommandOutcome, SmtpTransaction,
};
mod tls;
mod trace;
mod trace_actions;
pub(crate) use tls::smtp_starttls_acceptor_for_paths;
use tls::{smtp_starttls_acceptor_from_store, StartTlsStream};
use trace::{
    latest_decision_summary, quarantine_matches, quarantine_summary_from_message,
    trace_details_from_message,
};
pub(crate) use trace_actions::{delete_trace, load_trace_details, release_trace, retry_trace};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DecisionTraceEntry {
    stage: String,
    outcome: String,
    detail: String,
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

fn parse_peer_ip(peer: &str) -> Option<IpAddr> {
    if let Ok(addr) = peer.parse::<SocketAddr>() {
        return Some(addr.ip());
    }
    peer.parse::<IpAddr>().ok()
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

fn retry_after_seconds(base: u32, attempt_count: u32) -> u32 {
    let multiplier = 2u32.saturating_pow(attempt_count.min(4));
    base.max(1).saturating_mul(multiplier).min(3600)
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
