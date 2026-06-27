use super::*;

#[derive(Debug, Serialize, Deserialize)]
pub(in crate::smtp) struct TransportAuditEvent {
    pub(in crate::smtp) timestamp: String,
    pub(in crate::smtp) trace_id: String,
    pub(in crate::smtp) direction: String,
    pub(in crate::smtp) queue: String,
    pub(in crate::smtp) status: String,
    pub(in crate::smtp) peer: String,
    pub(in crate::smtp) mail_from: String,
    pub(in crate::smtp) rcpt_to: Vec<String>,
    pub(in crate::smtp) subject: String,
    pub(in crate::smtp) internet_message_id: Option<String>,
    pub(in crate::smtp) reason: Option<String>,
    pub(in crate::smtp) route_target: Option<String>,
    pub(in crate::smtp) remote_message_ref: Option<String>,
    pub(in crate::smtp) spam_score: f32,
    pub(in crate::smtp) security_score: f32,
    pub(in crate::smtp) reputation_score: i32,
    pub(in crate::smtp) dnsbl_hits: Vec<String>,
    pub(in crate::smtp) auth_summary: Value,
    pub(in crate::smtp) magika_summary: Option<String>,
    pub(in crate::smtp) magika_decision: Option<String>,
    pub(in crate::smtp) technical_status: Option<Value>,
    pub(in crate::smtp) dsn: Option<Value>,
    pub(in crate::smtp) throttle: Option<Value>,
    #[serde(default)]
    pub(in crate::smtp) message_size_bytes: Option<u64>,
    pub(in crate::smtp) decision_trace: Vec<Value>,
}

pub(in crate::smtp) async fn append_transport_audit(
    spool_dir: &Path,
    config: &RuntimeConfig,
    queue: &str,
    message: &QueuedMessage,
) -> Result<()> {
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
        internet_message_id: parse_rfc822_header_value(&message.data, "message-id"),
        reason: message.relay_error.clone(),
        route_target: message
            .route
            .as_ref()
            .and_then(|route| route.relay_target.clone()),
        remote_message_ref: message.remote_message_ref.clone(),
        spam_score: message.spam_score,
        security_score: message.security_score,
        reputation_score: message.reputation_score,
        dnsbl_hits: message.dnsbl_hits.clone(),
        auth_summary: serde_json::to_value(&message.auth_summary).unwrap_or(Value::Null),
        magika_summary: message.magika_summary.clone(),
        magika_decision: message.magika_decision.clone(),
        technical_status: serde_json::to_value(&message.technical_status).ok(),
        dsn: serde_json::to_value(&message.dsn).ok(),
        throttle: serde_json::to_value(&message.throttle).ok(),
        message_size_bytes: Some(message.data.len() as u64),
        decision_trace: message
            .decision_trace
            .iter()
            .filter_map(|entry| serde_json::to_value(entry).ok())
            .collect(),
    };
    let line = format!("{}\n", serde_json::to_string(&event)?);
    let path = spool_dir.join("policy").join("transport-audit.jsonl");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    use std::io::Write;
    file.write_all(line.as_bytes())?;
    if let Err(error) = append_postfix_style_mail_log(&event) {
        warn!(
            trace_id = %event.trace_id,
            error = %error,
            "unable to append postfix-style mail log"
        );
    }
    if let Some(pool) = ensure_local_db_schema(config).await? {
        persist_transport_audit_db_event(pool, &event).await?;
    }
    Ok(())
}

fn append_postfix_style_mail_log(event: &TransportAuditEvent) -> Result<()> {
    let Some(path) = postfix_style_mail_log_path() else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    use std::io::Write;
    file.write_all(postfix_style_mail_log_line(event).as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

fn postfix_style_mail_log_path() -> Option<PathBuf> {
    let enabled = env::var("LPE_CT_POSTFIX_MAIL_LOG_ENABLED")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false);
    if !enabled {
        return None;
    }

    env::var("LPE_CT_MAIL_LOG_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            env::var("LPE_CT_HOST_LOG_DIR")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(|dir| PathBuf::from(dir).join("mail.log"))
        })
}

pub(in crate::smtp) fn postfix_style_mail_log_line(event: &TransportAuditEvent) -> String {
    let pid = std::process::id();
    let host = env::var("LPE_CT_SERVER_NAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "lpe-ct".to_string());
    let recipients = event
        .rcpt_to
        .iter()
        .map(|recipient| sanitize_mail_log_value(recipient))
        .collect::<Vec<_>>()
        .join(",");
    let dsn_status = event
        .dsn
        .as_ref()
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let technical_detail = event
        .technical_status
        .as_ref()
        .and_then(|value| value.get("detail"))
        .and_then(Value::as_str)
        .unwrap_or("");

    format!(
        "{} {} lpe-ct/smtp[{}]: {}: direction={}, queue={}, status={}, from=<{}>, to=<{}>, peer={}, message-id=<{}>, size={}, relay={}, dsn={}, reason=\"{}\", reply=\"{}\", subject=\"{}\"",
        event.timestamp,
        sanitize_mail_log_value(&host),
        pid,
        sanitize_mail_log_value(&event.trace_id),
        sanitize_mail_log_value(&event.direction),
        sanitize_mail_log_value(&event.queue),
        sanitize_mail_log_value(&event.status),
        sanitize_mail_log_value(&event.mail_from),
        recipients,
        sanitize_mail_log_value(&event.peer),
        sanitize_mail_log_value(event.internet_message_id.as_deref().unwrap_or("")),
        event.message_size_bytes.unwrap_or(0),
        sanitize_mail_log_value(event.route_target.as_deref().unwrap_or("")),
        sanitize_mail_log_value(dsn_status),
        sanitize_mail_log_value(event.reason.as_deref().unwrap_or("")),
        sanitize_mail_log_value(technical_detail),
        sanitize_mail_log_value(&event.subject),
    )
}

fn sanitize_mail_log_value(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_control() || matches!(ch, '"' | '\r' | '\n') {
                ' '
            } else {
                ch
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

async fn persist_transport_audit_db_event(
    pool: &PgPool,
    event: &TransportAuditEvent,
) -> Result<()> {
    let event_unix = parse_unix_timestamp(&event.timestamp).unwrap_or(0) as i64;
    let event_key = transport_audit_event_key(event);
    sqlx::query(
        r#"
        INSERT INTO mail_flow_history (
            event_key, event_unix, timestamp, trace_id, direction, queue, status, peer, mail_from,
            rcpt_to, subject, internet_message_id, reason, route_target, remote_message_ref,
            spam_score, security_score, reputation_score, dnsbl_hits, auth_summary, magika_summary,
            magika_decision, technical_status, dsn, throttle, message_size_bytes, decision_trace, search_text
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20, $21,
            $22, $23, $24, $25, $26, $27, $28
        )
        ON CONFLICT (event_key) DO NOTHING
        "#,
    )
    .bind(&event_key)
    .bind(event_unix)
    .bind(&event.timestamp)
    .bind(&event.trace_id)
    .bind(&event.direction)
    .bind(&event.queue)
    .bind(&event.status)
    .bind(&event.peer)
    .bind(&event.mail_from)
    .bind(Json(event.rcpt_to.clone()))
    .bind(&event.subject)
    .bind(&event.internet_message_id)
    .bind(&event.reason)
    .bind(&event.route_target)
    .bind(&event.remote_message_ref)
    .bind(event.spam_score)
    .bind(event.security_score)
    .bind(event.reputation_score)
    .bind(Json(event.dnsbl_hits.clone()))
    .bind(Json(event.auth_summary.clone()))
    .bind(&event.magika_summary)
    .bind(&event.magika_decision)
    .bind(&event.technical_status)
    .bind(&event.dsn)
    .bind(&event.throttle)
    .bind(
        event
            .message_size_bytes
            .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
    )
    .bind(Json(event.decision_trace.clone()))
    .bind(transport_audit_search_text(event))
    .execute(pool)
    .await?;
    Ok(())
}

fn transport_audit_event_key(event: &TransportAuditEvent) -> String {
    let mut hasher = DefaultHasher::new();
    event.timestamp.hash(&mut hasher);
    event.trace_id.hash(&mut hasher);
    event.queue.hash(&mut hasher);
    event.status.hash(&mut hasher);
    event.reason.hash(&mut hasher);
    event.route_target.hash(&mut hasher);
    event.remote_message_ref.hash(&mut hasher);
    serde_json::to_string(&event.decision_trace)
        .unwrap_or_default()
        .hash(&mut hasher);
    format!("{}-{:x}", event.trace_id, hasher.finish())
}

fn transport_audit_search_text(event: &TransportAuditEvent) -> String {
    let mut parts = vec![
        event.trace_id.to_ascii_lowercase(),
        event.direction.to_ascii_lowercase(),
        event.queue.to_ascii_lowercase(),
        event.status.to_ascii_lowercase(),
        event.mail_from.to_ascii_lowercase(),
        event.subject.to_ascii_lowercase(),
        event.peer.to_ascii_lowercase(),
    ];
    parts.extend(event.rcpt_to.iter().map(|value| value.to_ascii_lowercase()));
    parts.extend(
        event
            .dnsbl_hits
            .iter()
            .map(|value| value.to_ascii_lowercase()),
    );
    if let Some(value) = &event.internet_message_id {
        parts.push(value.to_ascii_lowercase());
    }
    if let Some(value) = &event.reason {
        parts.push(value.to_ascii_lowercase());
    }
    if let Some(value) = &event.route_target {
        parts.push(value.to_ascii_lowercase());
    }
    if let Some(value) = &event.remote_message_ref {
        parts.push(value.to_ascii_lowercase());
    }
    parts.join(" ")
}

pub(in crate::smtp) fn quarantine_search_text(message: &QueuedMessage) -> String {
    let mut parts = vec![
        message.id.to_ascii_lowercase(),
        message.direction.to_ascii_lowercase(),
        message.status.to_ascii_lowercase(),
        message.peer.to_ascii_lowercase(),
        message.helo.to_ascii_lowercase(),
        message.mail_from.to_ascii_lowercase(),
        parse_rfc822_header_value(&message.data, "subject")
            .unwrap_or_default()
            .to_ascii_lowercase(),
    ];
    parts.extend(
        message
            .rcpt_to
            .iter()
            .map(|value| value.to_ascii_lowercase()),
    );
    parts.extend(
        message
            .dnsbl_hits
            .iter()
            .map(|value| value.to_ascii_lowercase()),
    );
    if let Some(value) = parse_rfc822_header_value(&message.data, "message-id") {
        parts.push(value.to_ascii_lowercase());
    }
    if let Some(value) = &message.relay_error {
        parts.push(value.to_ascii_lowercase());
    }
    if let Some(value) = message
        .route
        .as_ref()
        .and_then(|route| route.relay_target.as_ref())
    {
        parts.push(value.to_ascii_lowercase());
    }
    if let Some(value) = latest_decision_summary(&message.decision_trace) {
        parts.push(value.to_ascii_lowercase());
    }
    parts.join(" ")
}
