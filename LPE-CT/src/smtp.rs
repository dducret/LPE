use anyhow::{anyhow, Context, Result};
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
struct RuntimeConfig {
    primary_upstream: String,
    secondary_upstream: String,
    mutual_tls_required: bool,
    fallback_to_hold_queue: bool,
    drain_mode: bool,
    quarantine_enabled: bool,
    max_message_size_mb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueuedMessage {
    id: String,
    received_at: String,
    peer: String,
    helo: String,
    mail_from: String,
    rcpt_to: Vec<String>,
    status: String,
    relay_error: Option<String>,
    data: String,
}

pub(crate) fn initialize_spool(spool_dir: &Path) -> Result<()> {
    for queue in ["incoming", "deferred", "quarantine", "held", "sent"] {
        fs::create_dir_all(spool_dir.join(queue))
            .with_context(|| format!("unable to create spool queue {queue}"))?;
    }
    Ok(())
}

pub(crate) fn queue_metrics(spool_dir: &Path, upstream_reachable: bool) -> Result<super::QueueMetrics> {
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
            write_smtp(&mut writer, &format!("250 queued as {}", message.id)).await?;
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
    let mut message = QueuedMessage {
        id: message_id(),
        received_at: current_timestamp(),
        peer,
        helo,
        mail_from,
        rcpt_to,
        status: "incoming".to_string(),
        relay_error: None,
        data,
    };

    persist_message(spool_dir, "incoming", &message).await?;

    if config.drain_mode {
        message.status = "held".to_string();
        move_message(spool_dir, &message, "incoming", "held").await?;
        return Ok(message);
    }

    if config.quarantine_enabled && should_quarantine(&message.data) {
        message.status = "quarantined".to_string();
        move_message(spool_dir, &message, "incoming", "quarantine").await?;
        return Ok(message);
    }

    match relay_message(config, &message).await {
        Ok(()) => {
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

fn message_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("lpe-ct-{nanos}-{}", std::process::id())
}

fn current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format!("unix:{}", duration.as_secs()),
        Err(_) => "unix:0".to_string(),
    }
}
