use anyhow::{anyhow, Context, Result};
use lpe_magika::{
    collect_mime_attachment_parts, DetectionSource, Detector, ExpectedKind, IngressContext,
    MagikaDetection, PolicyDecision, ValidationRequest, Validator,
};
use lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, OutboundMessageHandoffRequest,
    OutboundMessageHandoffResponse, TransportDeliveryStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream},
};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    primary_upstream: String,
    secondary_upstream: String,
    core_delivery_base_url: String,
    mutual_tls_required: bool,
    fallback_to_hold_queue: bool,
    drain_mode: bool,
    quarantine_enabled: bool,
    max_message_size_mb: u32,
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
    data: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InboundMagikaOutcome {
    Accept,
    Quarantine(String),
    Reject(String),
}

pub(crate) fn initialize_spool(spool_dir: &Path) -> Result<()> {
    for queue in ["incoming", "deferred", "quarantine", "held", "sent", "outbound"] {
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
        data: compose_rfc822_message(&payload),
    };

    persist_message(spool_dir, "outbound", &message).await?;

    if config.quarantine_enabled && should_quarantine(&message.data) {
        message.status = "quarantined".to_string();
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
                        "554 message rejected by Magika policy (trace {})",
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
    data: String,
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
    data: String,
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
        data,
    };

    persist_message(spool_dir, "incoming", &message).await?;

    if config.drain_mode {
        message.status = "held".to_string();
        move_message(spool_dir, &message, "incoming", "held").await?;
        return Ok(message);
    }

    match classify_inbound_message(validator, message.data.as_bytes()) {
        Ok(InboundMagikaOutcome::Accept) => {}
        Ok(InboundMagikaOutcome::Quarantine(reason)) => {
            message.status = "quarantined".to_string();
            message.magika_decision = Some("quarantine".to_string());
            message.magika_summary = Some(reason);
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            return Ok(message);
        }
        Ok(InboundMagikaOutcome::Reject(reason)) => {
            message.status = "rejected".to_string();
            message.magika_decision = Some("reject".to_string());
            message.magika_summary = Some(reason);
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            return Ok(message);
        }
        Err(error) => {
            message.status = "quarantined".to_string();
            message.magika_decision = Some("quarantine".to_string());
            message.magika_summary = Some(format!("Magika validation failed: {error}"));
            move_message(spool_dir, &message, "incoming", "quarantine").await?;
            return Ok(message);
        }
    }

    if config.quarantine_enabled && should_quarantine(&message.data) {
        message.status = "quarantined".to_string();
        move_message(spool_dir, &message, "incoming", "quarantine").await?;
        return Ok(message);
    }

    match deliver_inbound_message(config, &message).await {
        Ok(_) => {
            message.status = "sent".to_string();
            move_message(spool_dir, &message, "incoming", "sent").await?;
        }
        Err(error) => {
            message.status = if config.fallback_to_hold_queue {
                "held".to_string()
            } else {
                "deferred".to_string()
            };
            message.relay_error = Some(error.to_string());
            let destination = if config.fallback_to_hold_queue {
                "held"
            } else {
                "deferred"
            };
            move_message(spool_dir, &message, "incoming", destination).await?;
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

async fn deliver_inbound_message(
    config: &RuntimeConfig,
    message: &QueuedMessage,
) -> Result<InboundDeliveryResponse> {
    let endpoint = format!(
        "{}/internal/lpe-ct/inbound-deliveries",
        config.core_delivery_base_url.trim_end_matches('/')
    );
    let subject = parse_header_value(&message.data, "subject").unwrap_or_default();
    let internet_message_id = parse_header_value(&message.data, "message-id");
    let body_text = extract_body_text(&message.data);
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
    let response = client
        .post(endpoint)
        .header(
            "x-lpe-integration-key",
            std::env::var("LPE_INTEGRATION_SHARED_SECRET")
                .unwrap_or_else(|_| "change-me".to_string()),
        )
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
        return Err(anyhow!("mutual TLS relay is configured but not implemented in LPE-CT v1"));
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
    writer.write_all(message.data.as_bytes()).await?;
    if !message.data.ends_with("\r\n") {
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

async fn read_smtp_data(reader: &mut BufReader<OwnedReadHalf>, max_mb: u32) -> Result<String> {
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
    Ok(String::from_utf8_lossy(&data).to_string())
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
    Ok(fs::read_dir(path)?.filter_map(std::result::Result::ok).count() as u32)
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

fn should_quarantine(data: &str) -> bool {
    data.lines().any(|line| {
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

fn compose_rfc822_message(payload: &OutboundMessageHandoffRequest) -> String {
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
                .map(|recipient| format_address(&recipient.address, recipient.display_name.as_deref()))
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
                .map(|recipient| format_address(&recipient.address, recipient.display_name.as_deref()))
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
    lines.push("Content-Type: text/plain; charset=utf-8".to_string());
    lines.push(String::new());
    lines.push(payload.body_text.clone());
    lines.join("\r\n")
}

fn format_address(address: &str, display_name: Option<&str>) -> String {
    match display_name.map(str::trim).filter(|value| !value.is_empty()) {
        Some(display_name) => format!("{display_name} <{address}>"),
        None => address.to_string(),
    }
}

fn parse_header_value(raw_message: &str, name: &str) -> Option<String> {
    let expected = format!("{}:", name.to_ascii_lowercase());
    let mut current = String::new();

    for line in raw_message.lines() {
        if line.trim().is_empty() {
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            current.push(' ');
            current.push_str(line.trim());
            continue;
        }
        if !current.is_empty() {
            let lower = current.to_ascii_lowercase();
            if lower.starts_with(&expected) {
                return current
                    .split_once(':')
                    .map(|(_, value)| value.trim().to_string())
                    .filter(|value| !value.is_empty());
            }
        }
        current = line.trim_end_matches('\r').to_string();
    }

    if !current.is_empty() {
        let lower = current.to_ascii_lowercase();
        if lower.starts_with(&expected) {
            return current
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .filter(|value| !value.is_empty());
        }
    }

    None
}

fn extract_body_text(raw_message: &str) -> String {
    raw_message
        .split_once("\r\n\r\n")
        .or_else(|| raw_message.split_once("\n\n"))
        .map(|(_, body)| body.to_string())
        .unwrap_or_default()
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
        classify_inbound_message, extract_body_text, initialize_spool, parse_header_value,
        process_outbound_handoff, receive_message, receive_message_with_validator, RuntimeConfig,
    };
    use axum::{routing::post, Json, Router};
    use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
    use lpe_domain::{
        InboundDeliveryRequest, InboundDeliveryResponse, OutboundMessageHandoffRequest,
        TransportDeliveryStatus, TransportRecipient,
    };
    use std::{
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
            max_message_size_mb: 16,
        }
    }

    #[derive(Debug, Clone)]
    struct FakeDetector {
        detection: Result<MagikaDetection, String>,
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
            self.detection
                .clone()
                .map_err(anyhow::Error::msg)
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
        assert!(spool.join("sent").join(format!("{}.json", response.trace_id)).exists());
        assert!(captured.lock().unwrap().contains("Subject: Relay test"));
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
        let spool = temp_dir("inbound-delivery");
        initialize_spool(&spool).unwrap();
        std::env::set_var("LPE_INTEGRATION_SHARED_SECRET", "integration-test");
        let captured = Arc::new(Mutex::new(None::<InboundDeliveryRequest>));
        let core_base_url = spawn_dummy_core(captured.clone()).await;

        let message = receive_message(
            &spool,
            &runtime_config("127.0.0.1:9".to_string(), core_base_url),
            "127.0.0.1:2525".to_string(),
            "example.test".to_string(),
            "sender@example.test".to_string(),
            vec!["dest@example.test".to_string()],
            "From: Sender <sender@example.test>\r\nSubject: Inbound\r\n\r\nBody".to_string(),
        )
        .await
        .unwrap();

        assert_eq!(message.status, "sent");
        assert!(spool.join("sent").join(format!("{}.json", message.id)).exists());
        let request = captured.lock().unwrap().clone().unwrap();
        assert_eq!(request.subject, "Inbound");
        assert_eq!(request.body_text, "Body");
        assert_eq!(request.rcpt_to, vec!["dest@example.test".to_string()]);
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
            .to_string(),
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
    fn raw_message_parsing_extracts_headers_and_body() {
        let raw = "Subject: Example\r\nMessage-Id: <id@test>\r\n\r\nBody";
        assert_eq!(parse_header_value(raw, "subject").as_deref(), Some("Example"));
        assert_eq!(parse_header_value(raw, "message-id").as_deref(), Some("<id@test>"));
        assert_eq!(extract_body_text(raw), "Body");
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

    async fn spawn_dummy_core(
        captured: Arc<Mutex<Option<InboundDeliveryRequest>>>,
    ) -> String {
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
