use lpe_domain::{
    TransportDeliveryStatus, TransportDsnReport, TransportRetryAdvice, TransportRouteDecision,
    TransportTechnicalStatus,
};

use super::{
    default_queue_for_status, retry_after_seconds, OutboundExecution, QueuedMessage,
    DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS,
};

pub(super) fn deferred_smtp_reply(message: &QueuedMessage) -> String {
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

pub(super) fn rejected_smtp_reply(message: &QueuedMessage) -> String {
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

pub(super) fn direct_mx_failure(
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

pub(super) fn is_permanent_direct_mx_error(detail: &str) -> bool {
    let lower = detail.to_ascii_lowercase();
    lower.contains("does not exist")
        || lower.contains("null mx")
        || lower.contains("does not accept mail")
        || lower.contains("recipient address has no domain")
        || lower.contains("no outbound recipients")
}

pub(super) fn is_permanent_relay_error(detail: &str) -> bool {
    let lower = detail.to_ascii_lowercase();
    lower.contains("mutual tls relay is configured but not implemented")
}

pub(super) fn parse_enhanced_status(detail: &str) -> Option<String> {
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
