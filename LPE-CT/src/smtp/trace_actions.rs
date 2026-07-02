use super::*;

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
