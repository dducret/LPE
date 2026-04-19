use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use email_auth::{
    common::dns::{DnsError, DnsResolver, MxRecord},
    dmarc::Disposition as DmarcDisposition,
    EmailAuthenticator,
};
use hickory_resolver::{
    proto::rr::RecordType,
    TokioResolver,
};
use lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, OutboundMessageHandoffRequest,
    OutboundMessageHandoffResponse, TransportDeliveryStatus,
};
use lpe_magika::{
    collect_mime_attachment_parts, extract_visible_text, parse_rfc822_header_value, Detector,
    ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs,
    hash::{Hash, Hasher},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream},
};
use tracing::{info, warn};

use crate::integration_shared_secret;

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
    require_spf: bool,
    require_dkim_alignment: bool,
    require_dmarc_enforcement: bool,
    dnsbl_enabled: bool,
    dnsbl_zones: Vec<String>,
    reputation_enabled: bool,
    spam_quarantine_threshold: f32,
    spam_reject_threshold: f32,
    max_message_size_mb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AuthSummary {
    spf: String,
    dkim: String,
    dmarc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecisionTraceEntry {
    stage: String,
    outcome: String,
    detail: String,
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
    #[serde(with = "base64_bytes")]
    data: Vec<u8>,
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

const GREYLIST_DELAY_SECONDS: u64 = 90;

pub(crate) fn initialize_spool(spool_dir: &Path) -> Result<()> {
    for queue in [
        "incoming",
        "deferred",
        "quarantine",
        "held",
        "sent",
        "outbound",
        "policy",
        "greylist",
    ] {
        fs::create_dir_all(spool_dir.join(queue))
            .with_context(|| format!("unable to create spool queue {queue}"))?;
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
        let state_file = state_file.clone();
        let spool_dir = spool_dir.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_smtp_session(stream, peer, state_file, spool_dir).await {
                warn!("smtp session failed for {peer}: {error}");
            }
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
        require_spf: dashboard.policies.require_spf,
        require_dkim_alignment: dashboard.policies.require_dkim_alignment,
        require_dmarc_enforcement: dashboard.policies.require_dmarc_enforcement,
        dnsbl_enabled: dashboard.policies.dnsbl_enabled,
        dnsbl_zones: dashboard.policies.dnsbl_zones.clone(),
        reputation_enabled: dashboard.policies.reputation_enabled,
        spam_quarantine_threshold: dashboard.policies.spam_quarantine_threshold,
        spam_reject_threshold: dashboard.policies.spam_reject_threshold,
        max_message_size_mb: dashboard.policies.max_message_size_mb,
    }
}

pub(crate) async fn process_outbound_handoff(
    spool_dir: &Path,
    config: &RuntimeConfig,
    payload: OutboundMessageHandoffRequest,
) -> Result<OutboundMessageHandoffResponse> {
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
        data: compose_rfc822_message(&payload),
    };

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
        return Ok(OutboundMessageHandoffResponse {
            queue_id: payload.queue_id,
            status: TransportDeliveryStatus::Quarantined,
            trace_id: message.id,
            detail: Some("message matched quarantine policy".to_string()),
        });
    }

    match relay_message(config, &message).await {
        Ok(()) => {
            message.status = "sent".to_string();
            message.decision_trace.push(DecisionTraceEntry {
                stage: "outbound-relay".to_string(),
                outcome: "relayed".to_string(),
                detail: "message relayed to upstream SMTP target".to_string(),
            });
            move_message(spool_dir, &message, "outbound", "sent").await?;
            Ok(OutboundMessageHandoffResponse {
                queue_id: payload.queue_id,
                status: TransportDeliveryStatus::Relayed,
                trace_id: message.id,
                detail: None,
            })
        }
        Err(error) => {
            let detail = error.to_string();
            let final_status = if is_permanent_relay_error(&detail) {
                TransportDeliveryStatus::Failed
            } else {
                TransportDeliveryStatus::Deferred
            };
            message.status = match final_status {
                TransportDeliveryStatus::Failed => "held".to_string(),
                TransportDeliveryStatus::Deferred => "deferred".to_string(),
                _ => "held".to_string(),
            };
            message.relay_error = Some(detail.clone());
            message.decision_trace.push(DecisionTraceEntry {
                stage: "outbound-relay".to_string(),
                outcome: final_status.as_str().to_string(),
                detail: detail.clone(),
            });
            let destination = if final_status == TransportDeliveryStatus::Deferred {
                "deferred"
            } else {
                "held"
            };
            move_message(spool_dir, &message, "outbound", destination).await?;
            Ok(OutboundMessageHandoffResponse {
                queue_id: payload.queue_id,
                status: final_status,
                trace_id: message.id,
                detail: Some(detail),
            })
        }
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
            } else {
                write_smtp(&mut writer, &format!("250 queued as {}", message.id)).await?;
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
            stage: "smtp-ingress".to_string(),
            outcome: "accepted".to_string(),
            detail: "message accepted by SMTP edge and persisted to the incoming spool".to_string(),
        }],
        data,
    };

    persist_message(spool_dir, "incoming", &message).await?;

    if config.drain_mode {
        message.status = "held".to_string();
        message.decision_trace.push(DecisionTraceEntry {
            stage: "drain-mode".to_string(),
            outcome: "held".to_string(),
            detail: "drain mode is enabled on the sorting center".to_string(),
        });
        move_message(spool_dir, &message, "incoming", "held").await?;
        return Ok(message);
    }

    match classify_inbound_message(validator, &message.data) {
        Ok(InboundMagikaOutcome::Accept) => {}
        Ok(InboundMagikaOutcome::Quarantine(reason)) => {
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
            return Ok(message);
        }
        Ok(InboundMagikaOutcome::Reject(reason)) => {
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
            return Ok(message);
        }
        Err(error) => {
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
                update_reputation(spool_dir, &message, FilterAction::Accept)?;
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
                update_reputation(spool_dir, &message, FilterAction::Defer)?;
            }
        }
        FilterAction::Quarantine => {
            message.status = "quarantined".to_string();
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            update_reputation(spool_dir, &message, FilterAction::Quarantine)?;
        }
        FilterAction::Reject => {
            message.status = "rejected".to_string();
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            update_reputation(spool_dir, &message, FilterAction::Reject)?;
        }
        FilterAction::Defer => {
            message.status = "deferred".to_string();
            move_message(spool_dir, &message, "incoming", "deferred").await?;
            update_reputation(spool_dir, &message, FilterAction::Defer)?;
        }
    }

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
    let mut dnsbl_hits = Vec::new();
    let mut auth_summary = AuthSummary::default();
    let reputation_score = if config.reputation_enabled {
        load_reputation_score(spool_dir, peer_ip, mail_from)?
    } else {
        0
    };

    if config.quarantine_enabled && should_quarantine(message_bytes) {
        decision_trace.push(DecisionTraceEntry {
            stage: "manual-quarantine".to_string(),
            outcome: "quarantine".to_string(),
            detail: "message matched the explicit quarantine marker policy".to_string(),
        });
        return Ok(FilterVerdict {
            action: FilterAction::Quarantine,
            reason: Some("message matched local quarantine policy".to_string()),
            spam_score: config.spam_quarantine_threshold.max(1.0),
            security_score: 1.0,
            reputation_score,
            dnsbl_hits,
            auth_summary,
            decision_trace,
        });
    }

    if let Some(ip) = peer_ip {
        if config.greylisting_enabled {
            match evaluate_greylisting(spool_dir, ip, mail_from, rcpt_to)? {
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
                        dnsbl_hits,
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
            dnsbl_hits = query_dnsbl_hits(ip, &config.dnsbl_zones).await;
            if !dnsbl_hits.is_empty() {
                spam_score += 4.0 + dnsbl_hits.len() as f32;
                security_score += 2.0;
                decision_trace.push(DecisionTraceEntry {
                    stage: "dnsbl".to_string(),
                    outcome: "listed".to_string(),
                    detail: format!("source IP listed on {}", dnsbl_hits.join(", ")),
                });
            } else {
                decision_trace.push(DecisionTraceEntry {
                    stage: "dnsbl".to_string(),
                    outcome: "clear".to_string(),
                    detail: "source IP not listed on configured DNSBL zones".to_string(),
                });
            }
        }

        match authenticate_message(ip, helo, mail_from, message_bytes).await {
            Ok((summary, auth_trace, action_hint)) => {
                auth_summary = summary;
                decision_trace.extend(auth_trace);
                match action_hint {
                    Some(FilterAction::Reject) => security_score += 4.0,
                    Some(FilterAction::Quarantine) => spam_score += 3.0,
                    Some(FilterAction::Defer) => security_score += 1.0,
                    _ => {}
                }
            }
            Err(error) => {
                security_score += 1.0;
                decision_trace.push(DecisionTraceEntry {
                    stage: "authentication".to_string(),
                    outcome: "temperror".to_string(),
                    detail: format!("authentication checks failed open with resolver error: {error}"),
                });
            }
        }
    } else {
        decision_trace.push(DecisionTraceEntry {
            stage: "authentication".to_string(),
            outcome: "skipped".to_string(),
            detail: "source peer IP could not be parsed for SPF, DKIM, and DMARC evaluation".to_string(),
        });
    }

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

    let reason = if config.require_dmarc_enforcement
        && auth_summary.dmarc.to_ascii_lowercase().contains("reject")
    {
        Some("DMARC policy requested reject".to_string())
    } else if config.require_spf
        && auth_summary.spf.to_ascii_lowercase().contains("fail")
        && auth_summary.dkim.to_ascii_lowercase().contains("none")
    {
        Some("SPF failed and no aligned DKIM signature passed".to_string())
    } else if config.require_dkim_alignment
        && !auth_summary.dkim.to_ascii_lowercase().contains("pass")
    {
        Some("aligned DKIM verification did not pass".to_string())
    } else if spam_score >= config.spam_reject_threshold {
        Some(format!(
            "spam score {:.1} reached reject threshold {:.1}",
            spam_score, config.spam_reject_threshold
        ))
    } else if spam_score >= config.spam_quarantine_threshold {
        Some(format!(
            "spam score {:.1} reached quarantine threshold {:.1}",
            spam_score, config.spam_quarantine_threshold
        ))
    } else {
        None
    };

    let action = if config.require_dmarc_enforcement
        && auth_summary.dmarc.to_ascii_lowercase().contains("reject")
    {
        FilterAction::Reject
    } else if config.require_spf
        && auth_summary.spf.to_ascii_lowercase().contains("fail")
        && auth_summary.dkim.to_ascii_lowercase().contains("none")
    {
        FilterAction::Reject
    } else if config.require_dkim_alignment
        && !auth_summary.dkim.to_ascii_lowercase().contains("pass")
    {
        FilterAction::Quarantine
    } else if spam_score >= config.spam_reject_threshold {
        FilterAction::Reject
    } else if spam_score >= config.spam_quarantine_threshold {
        FilterAction::Quarantine
    } else {
        FilterAction::Accept
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
            .unwrap_or_else(|| "message passed SMTP perimeter policy".to_string()),
    });

    Ok(FilterVerdict {
        action,
        reason,
        spam_score,
        security_score,
        reputation_score,
        dnsbl_hits,
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
    message.decision_trace.extend(verdict.decision_trace.clone());
    if let Some(reason) = &verdict.reason {
        message.relay_error = Some(reason.clone());
    }
}

async fn authenticate_message(
    client_ip: IpAddr,
    helo: &str,
    mail_from: &str,
    message_bytes: &[u8],
) -> Result<(AuthSummary, Vec<DecisionTraceEntry>, Option<FilterAction>)> {
    let authenticator = EmailAuthenticator::new(SystemDnsResolver::new()?, "lpe-ct.local");
    let result = authenticator
        .authenticate(message_bytes, client_ip, helo, mail_from)
        .await
        .map_err(|error| anyhow!("authentication evaluation failed: {error}"))?;

    let spf = format!("{:?}", result.spf);
    let dkim = format!("{:?}", result.dkim);
    let dmarc = format!("{:?}", result.dmarc.disposition);
    let mut trace = vec![
        DecisionTraceEntry {
            stage: "spf".to_string(),
            outcome: spf.clone(),
            detail: format!("SPF evaluation for {} from {}", mail_from, client_ip),
        },
        DecisionTraceEntry {
            stage: "dkim".to_string(),
            outcome: dkim.clone(),
            detail: "DKIM verification executed on the RFC 5322 message".to_string(),
        },
        DecisionTraceEntry {
            stage: "dmarc".to_string(),
            outcome: dmarc.clone(),
            detail: "DMARC evaluation executed from the RFC 5322 From domain".to_string(),
        },
    ];

    let action_hint = match result.dmarc.disposition {
        DmarcDisposition::Reject => Some(FilterAction::Reject),
        DmarcDisposition::Quarantine => Some(FilterAction::Quarantine),
        DmarcDisposition::TempFail => Some(FilterAction::Defer),
        _ => None,
    };
    if action_hint.is_none() && spf.to_ascii_lowercase().contains("softfail") {
        trace.push(DecisionTraceEntry {
            stage: "spf".to_string(),
            outcome: "softfail".to_string(),
            detail: "SPF softfail contributes to the spam score but does not reject by itself"
                .to_string(),
        });
    }

    Ok((
        AuthSummary { spf, dkim, dmarc },
        trace,
        action_hint,
    ))
}

async fn query_dnsbl_hits(ip: IpAddr, zones: &[String]) -> Vec<String> {
    let resolver = match SystemDnsResolver::new() {
        Ok(resolver) => resolver,
        Err(_) => return Vec::new(),
    };
    let mut hits = Vec::new();
    for zone in zones {
        let query = dnsbl_query_name(ip, zone);
        if resolver.query_exists(&query).await.unwrap_or(false) {
            hits.push(zone.clone());
        }
    }
    hits
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
            format!("{}.{}", hex.into_iter().rev().collect::<Vec<_>>().join("."), zone)
        }
    }
}

fn parse_peer_ip(peer: &str) -> Option<IpAddr> {
    if let Ok(addr) = peer.parse::<SocketAddr>() {
        return Some(addr.ip());
    }
    peer.parse::<IpAddr>().ok()
}

fn evaluate_greylisting(
    spool_dir: &Path,
    ip: IpAddr,
    mail_from: &str,
    rcpt_to: &[String],
) -> Result<Option<String>> {
    let rcpt = rcpt_to.first().map(String::as_str).unwrap_or_default();
    let key = stable_key_id(&(ip, mail_from.to_ascii_lowercase(), rcpt.to_ascii_lowercase()));
    let path = spool_dir.join("greylist").join(format!("{key}.json"));
    let now = unix_now();
    let mut entry = if path.exists() {
        serde_json::from_str::<GreylistEntry>(&fs::read_to_string(&path)?)?
    } else {
        GreylistEntry {
            first_seen_unix: now,
            release_after_unix: now + GREYLIST_DELAY_SECONDS,
            pass_count: 0,
        }
    };

    if now < entry.release_after_unix {
        if !path.exists() {
            fs::write(&path, serde_json::to_string_pretty(&entry)?)?;
        }
        return Ok(Some(format!(
            "greylisted triplet {} for {} seconds",
            key, GREYLIST_DELAY_SECONDS
        )));
    }

    entry.pass_count += 1;
    fs::write(&path, serde_json::to_string_pretty(&entry)?)?;
    Ok(None)
}

fn load_reputation_score(spool_dir: &Path, peer_ip: Option<IpAddr>, mail_from: &str) -> Result<i32> {
    let store = load_reputation_store(spool_dir)?;
    let key = reputation_key(peer_ip, mail_from);
    let entry = store.entries.get(&key).cloned().unwrap_or_default();
    Ok(entry.accepted as i32 - (entry.quarantined as i32 * 2) - (entry.rejected as i32 * 3))
}

fn update_reputation(spool_dir: &Path, message: &QueuedMessage, action: FilterAction) -> Result<()> {
    let mut store = load_reputation_store(spool_dir)?;
    let key = reputation_key(parse_peer_ip(&message.peer), &message.mail_from);
    let entry = store.entries.entry(key).or_default();
    match action {
        FilterAction::Accept => entry.accepted += 1,
        FilterAction::Quarantine => entry.quarantined += 1,
        FilterAction::Reject => entry.rejected += 1,
        FilterAction::Defer => entry.deferred += 1,
    }
    save_reputation_store(spool_dir, &store)
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
        let lookup = self.resolver.ipv4_lookup(name).await.map_err(map_dns_error)?;
        Ok(lookup.iter().map(|record| record.0).collect())
    }

    async fn query_aaaa(&self, name: &str) -> Result<Vec<Ipv6Addr>, DnsError> {
        let lookup = self.resolver.ipv6_lookup(name).await.map_err(map_dns_error)?;
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
        "{}/internal/lpe-ct/inbound-deliveries",
        config.core_delivery_base_url.trim_end_matches('/')
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
    let response = client
        .post(endpoint)
        .header("x-lpe-integration-key", integration_secret)
        .json(&request)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("core delivery endpoint returned {status}: {body}"));
    }

    let delivery: InboundDeliveryResponse = response.json().await?;
    if delivery.accepted_recipients.is_empty() {
        return Err(anyhow!(
            "core delivery rejected all recipients: {:?}",
            delivery.rejected_recipients
        ));
    }
    Ok(delivery)
}

async fn relay_message(config: &RuntimeConfig, message: &QueuedMessage) -> Result<()> {
    if config.mutual_tls_required {
        return Err(anyhow!(
            "mutual TLS relay is configured but not implemented in LPE-CT v1"
        ));
    }

    let mut last_error = None;
    for target in [&config.primary_upstream, &config.secondary_upstream] {
        if target.trim().is_empty() {
            continue;
        }

        match relay_message_to_target(target, message).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no relay target configured")))
}

async fn relay_message_to_target(target: &str, message: &QueuedMessage) -> Result<()> {
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
        smtp_command(
            &mut reader,
            &mut writer,
            &format!("RCPT TO:<{}>", recipient),
            250,
        )
        .await?;
    }
    smtp_command(&mut reader, &mut writer, "DATA", 354).await?;
    writer.write_all(&message.data).await?;
    if !message.data.ends_with(b"\r\n") {
        writer.write_all(b"\r\n").await?;
    }
    writer.write_all(b".\r\n").await?;
    expect_smtp(&mut reader, 250).await?;
    writer.write_all(b"QUIT\r\n").await?;
    Ok(())
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

async fn expect_smtp(reader: &mut BufReader<OwnedReadHalf>, expected: u16) -> Result<()> {
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line).await?;
        if line.len() < 3 {
            return Err(anyhow!("invalid SMTP response"));
        }
        let code = line[0..3].parse::<u16>().unwrap_or(0);
        let more = line.as_bytes().get(3) == Some(&b'-');
        if !more {
            if code == expected {
                return Ok(());
            }
            return Err(anyhow!("unexpected SMTP response: {}", line.trim_end()));
        }
    }
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
    tokio::fs::write(
        spool_path(spool_dir, queue, &message.id),
        serde_json::to_string_pretty(message)?,
    )
    .await?;
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
        require_spf: bool_at(&value, &["policies", "require_spf"], true),
        require_dkim_alignment: bool_at(
            &value,
            &["policies", "require_dkim_alignment"],
            false,
        ),
        require_dmarc_enforcement: bool_at(
            &value,
            &["policies", "require_dmarc_enforcement"],
            true,
        ),
        dnsbl_enabled: bool_at(&value, &["policies", "dnsbl_enabled"], true),
        dnsbl_zones: strings_at(
            &value,
            &["policies", "dnsbl_zones"],
            &["zen.spamhaus.org", "bl.spamcop.net"],
        ),
        reputation_enabled: bool_at(&value, &["policies", "reputation_enabled"], true),
        spam_quarantine_threshold: f32_at(
            &value,
            &["policies", "spam_quarantine_threshold"],
            5.0,
        ),
        spam_reject_threshold: f32_at(&value, &["policies", "spam_reject_threshold"], 9.0),
        max_message_size_mb: u32_at(&value, &["policies", "max_message_size_mb"], 64),
    })
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
        classify_inbound_message, compose_rfc822_message, dnsbl_query_name,
        encode_quoted_printable, evaluate_greylisting, initialize_spool,
        load_reputation_score, parse_peer_ip, process_outbound_handoff, receive_message,
        receive_message_with_validator, stable_key_id, unix_now, update_reputation, AuthSummary,
        FilterAction, GreylistEntry, QueuedMessage, RuntimeConfig,
    };
    use axum::{routing::post, Json, Router};
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

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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
            require_spf: true,
            require_dkim_alignment: false,
            require_dmarc_enforcement: true,
            dnsbl_enabled: false,
            dnsbl_zones: Vec::new(),
            reputation_enabled: true,
            spam_quarantine_threshold: 5.0,
            spam_reject_threshold: 9.0,
            max_message_size_mb: 16,
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
            },
        )
        .await
        .unwrap();

        assert_eq!(response.status, TransportDeliveryStatus::Relayed);
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
    async fn inbound_message_posts_to_core_delivery_api() {
        let _guard = ENV_LOCK.lock().unwrap();
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
    async fn inbound_message_keeps_non_utf8_raw_bytes() {
        let _guard = ENV_LOCK.lock().unwrap();
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

    #[test]
    fn greylisting_defers_first_triplet_then_allows_after_release_window() {
        let spool = temp_dir("greylisting");
        initialize_spool(&spool).unwrap();
        let ip: IpAddr = "192.0.2.45".parse().unwrap();
        let rcpt = vec!["dest@example.test".to_string()];

        let first = evaluate_greylisting(&spool, ip, "sender@example.test", &rcpt).unwrap();
        assert!(first.unwrap().contains("greylisted triplet"));

        let key = stable_key_id(&(ip, "sender@example.test".to_string(), "dest@example.test".to_string()));
        let path = spool.join("greylist").join(format!("{key}.json"));
        let mut entry: GreylistEntry = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        entry.release_after_unix = unix_now().saturating_sub(1);
        std::fs::write(&path, serde_json::to_string_pretty(&entry).unwrap()).unwrap();

        let second = evaluate_greylisting(&spool, ip, "sender@example.test", &rcpt).unwrap();
        assert!(second.is_none());
    }

    #[test]
    fn reputation_score_penalizes_quarantine_and_rejects() {
        let spool = temp_dir("reputation");
        initialize_spool(&spool).unwrap();
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
            data: b"Subject: test\r\n\r\nbody".to_vec(),
        };

        update_reputation(&spool, &message, FilterAction::Accept).unwrap();
        update_reputation(&spool, &message, FilterAction::Quarantine).unwrap();
        message.id = "trace-2".to_string();
        update_reputation(&spool, &message, FilterAction::Reject).unwrap();

        let score = load_reputation_score(&spool, parse_peer_ip(&message.peer), &message.mail_from).unwrap();
        assert_eq!(score, -4);
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_dummy_smtp(stream, captured).await;
        });
        address.to_string()
    }

    async fn handle_dummy_smtp(stream: TcpStream, captured: Arc<Mutex<String>>) {
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
                *captured.lock().unwrap() = data;
                writer.write_all(b"250 stored\r\n").await.unwrap();
            } else if trimmed == "QUIT" {
                writer.write_all(b"221 bye\r\n").await.unwrap();
                break;
            } else {
                writer.write_all(b"250 ok\r\n").await.unwrap();
            }
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
                trace_id: request.trace_id,
                status: TransportDeliveryStatus::Relayed,
                accepted_recipients: request.rcpt_to.clone(),
                rejected_recipients: Vec::new(),
                stored_message_ids: Vec::new(),
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
