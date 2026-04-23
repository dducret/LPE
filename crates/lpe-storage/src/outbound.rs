use anyhow::{anyhow, Result};
use lpe_domain::{
    OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, TransportDeliveryStatus,
    TransportRecipient, TransportRetryAdvice,
};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    JmapEmailRecipientRow, MessageBccRecipientRow, OutboundQueueStateRow,
    OutboundQueueStatusUpdate, PendingOutboundQueueRow, Storage,
};

fn queue_status_is_terminal(status: &str) -> bool {
    matches!(status, "relayed" | "quarantined" | "bounced" | "failed")
}

fn same_trace_id(current_trace_id: Option<&str>, response_trace_id: &str) -> bool {
    current_trace_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        == Some(response_trace_id.trim())
}

fn is_duplicate_terminal_handoff(
    current_status: &str,
    current_remote_message_ref: Option<&str>,
    response: &OutboundMessageHandoffResponse,
) -> bool {
    current_status == response.status.as_str()
        && queue_status_is_terminal(current_status)
        && current_remote_message_ref
            .map(str::trim)
            .filter(|value| !value.is_empty())
            == response
                .remote_message_ref
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
}

fn would_regress_queue_status(current_status: &str, next_status: &str) -> bool {
    matches!((current_status, next_status), ("deferred", "queued"))
}

fn synthesized_retry_policy(response: &OutboundMessageHandoffResponse) -> &'static str {
    if response.trace_id.trim().starts_with("lpe-dispatch-") {
        "dispatch-backoff"
    } else {
        "deferred-backoff"
    }
}

fn default_retry_after_seconds(attempts: i32) -> i32 {
    let next_attempt = attempts.saturating_add(1).max(1);
    next_attempt.saturating_mul(300).min(3600)
}

fn normalize_handoff_response(
    attempts: i32,
    response: &OutboundMessageHandoffResponse,
) -> OutboundMessageHandoffResponse {
    let mut normalized = response.clone();
    normalized.trace_id = normalized.trace_id.trim().to_string();
    normalized.remote_message_ref = normalized
        .remote_message_ref
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);

    normalized.retry = match normalized.status {
        TransportDeliveryStatus::Deferred => {
            let retry = normalized
                .retry
                .take()
                .unwrap_or_else(|| TransportRetryAdvice {
                    retry_after_seconds: default_retry_after_seconds(attempts) as u32,
                    policy: synthesized_retry_policy(response).to_string(),
                    reason: normalized.detail.clone(),
                });
            Some(TransportRetryAdvice {
                retry_after_seconds: retry.retry_after_seconds.max(1),
                policy: retry.policy.trim().to_string(),
                reason: retry
                    .reason
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string),
            })
        }
        _ => None,
    };

    normalized
}

impl Storage {
    pub async fn fetch_outbound_handoff_batch(
        &self,
        limit: i64,
    ) -> Result<Vec<OutboundMessageHandoffRequest>> {
        let rows = sqlx::query_as::<_, PendingOutboundQueueRow>(
            r#"
            SELECT
                q.id AS queue_id,
                q.message_id,
                q.account_id,
                q.attempts,
                m.from_address,
                m.from_display,
                m.sender_address,
                m.sender_display,
                m.sender_authorization_kind,
                m.subject_normalized AS subject,
                b.body_text,
                b.body_html_sanitized,
                m.internet_message_id,
                q.last_error
            FROM outbound_message_queue q
            JOIN messages m ON m.id = q.message_id
            JOIN message_bodies b ON b.message_id = m.id
            WHERE q.status IN ('queued', 'deferred')
              AND q.next_attempt_at <= NOW()
            ORDER BY q.created_at ASC, q.id ASC
            LIMIT $1
            "#,
        )
        .bind(limit.max(1))
        .fetch_all(&self.pool)
        .await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let tenant_id = self.tenant_id_for_account_id(row.account_id).await?;
            let recipients = sqlx::query_as::<_, JmapEmailRecipientRow>(
                r#"
                SELECT
                    r.message_id,
                    r.kind,
                    r.address,
                    r.display_name,
                    r.ordinal AS _ordinal
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = $2
                ORDER BY r.kind ASC, r.ordinal ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(row.message_id)
            .fetch_all(&self.pool)
            .await?;

            let bcc = sqlx::query_as::<_, MessageBccRecipientRow>(
                r#"
                SELECT address, display_name
                FROM message_bcc_recipients
                WHERE tenant_id = $1 AND message_id = $2
                ORDER BY ordinal ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(row.message_id)
            .fetch_all(&self.pool)
            .await?;

            let to = recipients
                .iter()
                .filter(|recipient| recipient.kind == "to")
                .map(|recipient| TransportRecipient {
                    address: recipient.address.clone(),
                    display_name: recipient.display_name.clone(),
                })
                .collect();
            let cc = recipients
                .iter()
                .filter(|recipient| recipient.kind == "cc")
                .map(|recipient| TransportRecipient {
                    address: recipient.address.clone(),
                    display_name: recipient.display_name.clone(),
                })
                .collect();
            let bcc = bcc
                .into_iter()
                .map(|recipient| TransportRecipient {
                    address: recipient.address,
                    display_name: recipient.display_name,
                })
                .collect();

            items.push(OutboundMessageHandoffRequest {
                queue_id: row.queue_id,
                message_id: row.message_id,
                account_id: row.account_id,
                from_address: row.from_address,
                from_display: row.from_display,
                sender_address: row.sender_address,
                sender_display: row.sender_display,
                sender_authorization_kind: row.sender_authorization_kind,
                to,
                cc,
                bcc,
                subject: row.subject,
                body_text: row.body_text,
                body_html_sanitized: row.body_html_sanitized,
                internet_message_id: row.internet_message_id,
                attempt_count: row.attempts.max(0) as u32,
                last_attempt_error: row.last_error,
            });
        }

        Ok(items)
    }

    pub async fn update_outbound_queue_status(
        &self,
        response: &OutboundMessageHandoffResponse,
    ) -> Result<OutboundQueueStatusUpdate> {
        let current = sqlx::query_as::<_, OutboundQueueStateRow>(
            r#"
            SELECT
                tenant_id,
                message_id,
                status,
                attempts,
                last_trace_id,
                remote_message_ref,
                retry_after_seconds,
                retry_policy,
                last_result_json
            FROM outbound_message_queue
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(response.queue_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("outbound queue item not found"))?;

        if same_trace_id(current.last_trace_id.as_deref(), &response.trace_id)
            || is_duplicate_terminal_handoff(
                &current.status,
                current.remote_message_ref.as_deref(),
                response,
            )
            || queue_status_is_terminal(&current.status)
            || would_regress_queue_status(&current.status, response.status.as_str())
        {
            return Ok(OutboundQueueStatusUpdate {
                queue_id: response.queue_id,
                message_id: current.message_id,
                status: current.status,
                trace_id: current.last_trace_id,
                remote_message_ref: current.remote_message_ref,
                retry_after_seconds: current.retry_after_seconds,
                retry_policy: current.retry_policy,
                technical_status: current.last_result_json,
            });
        }

        let normalized = normalize_handoff_response(current.attempts, response);
        let status_value = normalized.status.as_str().to_string();
        let retry_after_seconds = normalized
            .retry
            .as_ref()
            .map(|retry| retry.retry_after_seconds.min(i32::MAX as u32) as i32);
        let retry_policy = normalized.retry.as_ref().map(|retry| retry.policy.clone());
        let technical_status = serde_json::to_value(&normalized)?;
        let row = sqlx::query(
            r#"
            UPDATE outbound_message_queue
            SET status = $3,
                attempts = attempts + 1,
                next_attempt_at = CASE
                    WHEN $3 = 'deferred'
                        THEN NOW() + make_interval(secs => GREATEST(1, COALESCE($4, LEAST(3600, GREATEST(1, attempts + 1) * 300))))
                    ELSE NOW()
                END,
                last_error = CASE
                    WHEN $3 = 'relayed' THEN NULL
                    ELSE $5
                END,
                remote_message_ref = COALESCE($6, remote_message_ref),
                last_result_json = $7,
                last_attempt_at = NOW(),
                retry_after_seconds = $4,
                retry_policy = $8,
                last_dsn_action = $9,
                last_dsn_status = $10,
                last_smtp_code = $11,
                last_enhanced_status = $12,
                last_routing_rule = $13,
                last_throttle_scope = $14,
                last_throttle_delay_seconds = $15,
                last_trace_id = $16,
                updated_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            RETURNING message_id, status, last_trace_id, remote_message_ref, retry_after_seconds, retry_policy, last_result_json
            "#,
        )
        .bind(&current.tenant_id)
        .bind(response.queue_id)
        .bind(&status_value)
        .bind(retry_after_seconds)
        .bind(normalized.detail.as_deref())
        .bind(normalized.remote_message_ref.as_deref())
        .bind(&technical_status)
        .bind(retry_policy.as_deref())
        .bind(normalized.dsn.as_ref().map(|dsn| dsn.action.as_str()))
        .bind(normalized.dsn.as_ref().map(|dsn| dsn.status.as_str()))
        .bind(normalized.technical.as_ref().and_then(|status| status.smtp_code.map(i32::from)))
        .bind(
            normalized
                .technical
                .as_ref()
                .and_then(|status| status.enhanced_code.as_deref()),
        )
        .bind(normalized.route.as_ref().and_then(|route| route.rule_id.as_deref()))
        .bind(
            normalized
                .throttle
                .as_ref()
                .map(|throttle| throttle.scope.as_str()),
        )
        .bind(normalized.throttle.as_ref().map(|throttle| {
            throttle.retry_after_seconds.min(i32::MAX as u32) as i32
        }))
        .bind(&normalized.trace_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("outbound queue item not found"))?;

        let message_id: Uuid = row.try_get("message_id")?;
        let stored_status: String = row.try_get("status")?;
        let stored_trace_id: Option<String> = row.try_get("last_trace_id")?;
        let stored_remote_message_ref: Option<String> = row.try_get("remote_message_ref")?;
        let stored_retry_after_seconds: Option<i32> = row.try_get("retry_after_seconds")?;
        let stored_retry_policy: Option<String> = row.try_get("retry_policy")?;
        let stored_technical_status: Value = row.try_get("last_result_json")?;

        sqlx::query(
            r#"
            UPDATE messages
            SET delivery_status = $3
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(&current.tenant_id)
        .bind(message_id)
        .bind(&stored_status)
        .execute(&self.pool)
        .await?;

        Ok(OutboundQueueStatusUpdate {
            queue_id: response.queue_id,
            message_id,
            status: stored_status,
            trace_id: stored_trace_id,
            remote_message_ref: stored_remote_message_ref,
            retry_after_seconds: stored_retry_after_seconds,
            retry_policy: stored_retry_policy,
            technical_status: stored_technical_status,
        })
    }

    pub async fn mark_outbound_queue_attempt_failure(
        &self,
        queue_id: Uuid,
        detail: &str,
    ) -> Result<OutboundQueueStatusUpdate> {
        self.update_outbound_queue_status(&OutboundMessageHandoffResponse {
            queue_id,
            status: TransportDeliveryStatus::Deferred,
            trace_id: format!("lpe-dispatch-{queue_id}"),
            detail: Some(detail.to_string()),
            remote_message_ref: None,
            retry: None,
            dsn: None,
            technical: None,
            route: None,
            throttle: None,
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use lpe_domain::{TransportDeliveryStatus, TransportTechnicalStatus};

    use super::*;

    fn response(
        status: TransportDeliveryStatus,
        trace_id: &str,
        remote_message_ref: Option<&str>,
    ) -> OutboundMessageHandoffResponse {
        OutboundMessageHandoffResponse {
            queue_id: Uuid::nil(),
            status,
            trace_id: trace_id.to_string(),
            detail: Some("detail".to_string()),
            remote_message_ref: remote_message_ref.map(ToString::to_string),
            retry: None,
            dsn: None,
            technical: Some(TransportTechnicalStatus {
                phase: "data".to_string(),
                smtp_code: Some(250),
                enhanced_code: Some("2.0.0".to_string()),
                remote_host: Some("relay.example.test".to_string()),
                detail: Some("ok".to_string()),
            }),
            route: None,
            throttle: None,
        }
    }

    #[test]
    fn duplicate_handoff_is_recognized_by_trace_id_even_when_status_differs() {
        let response = response(TransportDeliveryStatus::Deferred, "trace-1", None);

        assert!(same_trace_id(Some("trace-1"), &response.trace_id));
        assert!(same_trace_id(Some(" trace-1 "), "trace-1"));
        assert!(same_trace_id(Some("trace-1"), " trace-1 "));
    }

    #[test]
    fn duplicate_terminal_handoff_is_recognized_by_remote_reference() {
        let response = response(
            TransportDeliveryStatus::Relayed,
            "trace-2",
            Some("remote-123"),
        );

        assert!(is_duplicate_terminal_handoff(
            "relayed",
            Some("remote-123"),
            &response,
        ));
    }

    #[test]
    fn terminal_queue_states_do_not_regress() {
        assert!(queue_status_is_terminal("relayed"));
        assert!(queue_status_is_terminal("failed"));
        assert!(!queue_status_is_terminal("queued"));
        assert!(!queue_status_is_terminal("deferred"));
    }

    #[test]
    fn deferred_queue_state_does_not_regress_to_queued() {
        assert!(would_regress_queue_status("deferred", "queued"));
        assert!(!would_regress_queue_status("queued", "deferred"));
        assert!(!would_regress_queue_status("queued", "relayed"));
    }

    #[test]
    fn deferred_responses_without_retry_get_default_guidance() {
        let normalized = normalize_handoff_response(
            0,
            &OutboundMessageHandoffResponse {
                queue_id: Uuid::nil(),
                status: TransportDeliveryStatus::Deferred,
                trace_id: " lpe-dispatch-test ".to_string(),
                detail: Some("connection lost".to_string()),
                remote_message_ref: Some("  ".to_string()),
                retry: None,
                dsn: None,
                technical: None,
                route: None,
                throttle: None,
            },
        );

        assert_eq!(normalized.trace_id, "lpe-dispatch-test");
        assert_eq!(normalized.remote_message_ref, None);
        assert_eq!(
            normalized.retry,
            Some(TransportRetryAdvice {
                retry_after_seconds: 300,
                policy: "dispatch-backoff".to_string(),
                reason: Some("connection lost".to_string()),
            })
        );
    }

    #[test]
    fn terminal_responses_clear_retry_guidance() {
        let normalized = normalize_handoff_response(
            2,
            &OutboundMessageHandoffResponse {
                queue_id: Uuid::nil(),
                status: TransportDeliveryStatus::Failed,
                trace_id: "trace-9".to_string(),
                detail: Some("permanent failure".to_string()),
                remote_message_ref: None,
                retry: Some(TransportRetryAdvice {
                    retry_after_seconds: 120,
                    policy: "unexpected".to_string(),
                    reason: Some("should be cleared".to_string()),
                }),
                dsn: None,
                technical: None,
                route: None,
                throttle: None,
            },
        );

        assert_eq!(normalized.retry, None);
    }
}
