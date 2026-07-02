use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ThrottleState {
    hits: Vec<u64>,
}

pub(in crate::smtp) fn outbound_handoff_response_from_spool(
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

pub(in crate::smtp) fn resolve_outbound_route(
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

pub(in crate::smtp) async fn evaluate_outbound_throttle(
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

pub(in crate::smtp) fn default_queue_for_status(status: &TransportDeliveryStatus) -> &'static str {
    match status {
        TransportDeliveryStatus::Relayed => "sent",
        TransportDeliveryStatus::Deferred => "deferred",
        TransportDeliveryStatus::Quarantined => "quarantine",
        TransportDeliveryStatus::Bounced => "bounces",
        TransportDeliveryStatus::Queued => "outbound",
        TransportDeliveryStatus::Failed => "held",
    }
}
