use super::*;

pub(in crate::smtp) fn quarantine_summary_from_message(
    message: &QueuedMessage,
) -> QuarantineSummary {
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

pub(in crate::smtp) fn latest_decision_summary(trace: &[DecisionTraceEntry]) -> Option<String> {
    trace
        .last()
        .map(|entry| format!("{}:{}", entry.stage, entry.outcome))
}

pub(in crate::smtp) fn quarantine_matches(
    item: &QuarantineSummary,
    query: &QuarantineQuery,
) -> bool {
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

pub(in crate::smtp) fn trace_details_from_message(
    queue: &str,
    message: &QueuedMessage,
) -> TraceDetails {
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
