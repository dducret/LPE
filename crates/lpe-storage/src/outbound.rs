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
    matches!(status, "relayed" | "bounced" | "failed" | "cancelled")
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

fn should_ignore_handoff_response(
    current_status: &str,
    current_trace_id: Option<&str>,
    current_remote_message_ref: Option<&str>,
    response: &OutboundMessageHandoffResponse,
) -> bool {
    (current_status == response.status.as_str()
        && same_trace_id(current_trace_id, &response.trace_id))
        || is_duplicate_terminal_handoff(current_status, current_remote_message_ref, response)
        || queue_status_is_terminal(current_status)
        || would_regress_queue_status(current_status, response.status.as_str())
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
    next_attempt.saturating_mul(60).min(3600)
}

fn submission_queue_status(status: &str) -> &str {
    match status {
        "quarantined" => "failed",
        other => other,
    }
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
                m.id AS message_id,
                q.account_id,
                q.attempts,
                q.from_address,
                fr.display_name AS from_display,
                q.sender_address,
                sr.display_name AS sender_display,
                q.authorization_kind AS sender_authorization_kind,
                m.normalized_subject AS subject,
                COALESCE(tb.body_text, '') AS body_text,
                hb.sanitized_html AS body_html_sanitized,
                m.internet_message_id,
                q.last_error
            FROM submission_queue q
            JOIN mailbox_messages mm
              ON mm.tenant_id = q.tenant_id
             AND mm.account_id = q.account_id
             AND mm.id = q.sent_mailbox_message_id
            JOIN messages m
              ON m.tenant_id = mm.tenant_id
             AND m.id = mm.message_id
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id AND fr.message_id = m.id AND fr.role = 'from'
            LEFT JOIN message_recipients sr
              ON sr.tenant_id = m.tenant_id AND sr.message_id = m.id AND sr.role = 'sender'
            LEFT JOIN LATERAL (
                SELECT body_text
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'text'
                LIMIT 1
            ) tb ON TRUE
            LEFT JOIN LATERAL (
                SELECT sanitized_html
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'html'
                LIMIT 1
            ) hb ON TRUE
            WHERE q.status IN ('queued', 'ready', 'deferred')
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
                    r.role AS kind,
                    r.address,
                    r.display_name,
                    r.ordinal AS _ordinal
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = $2
                ORDER BY r.role ASC, r.ordinal ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(row.message_id)
            .fetch_all(&self.pool)
            .await?;

            let bcc = sqlx::query_as::<_, MessageBccRecipientRow>(
                r#"
                SELECT address, display_name
                FROM protected_bcc_recipients
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
                q.tenant_id,
                q.account_id,
                mm.message_id,
                q.status,
                q.attempts,
                q.last_trace_id,
                q.remote_message_ref,
                NULL::integer AS retry_after_seconds,
                NULL::text AS retry_policy,
                jsonb_build_object(
                    'status', q.status,
                    'traceId', q.last_trace_id,
                    'remoteMessageRef', q.remote_message_ref,
                    'lastError', q.last_error
                ) AS last_result_json
            FROM submission_queue q
            JOIN mailbox_messages mm
              ON mm.tenant_id = q.tenant_id
             AND mm.account_id = q.account_id
             AND mm.id = q.sent_mailbox_message_id
            WHERE q.id = $1
            LIMIT 1
            "#,
        )
        .bind(response.queue_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("outbound queue item not found"))?;

        if should_ignore_handoff_response(
            &current.status,
            current.last_trace_id.as_deref(),
            current.remote_message_ref.as_deref(),
            response,
        ) {
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
        let status_value = submission_queue_status(normalized.status.as_str()).to_string();
        let retry_after_seconds = normalized
            .retry
            .as_ref()
            .map(|retry| retry.retry_after_seconds.min(i32::MAX as u32) as i32);
        let technical_status = serde_json::to_value(&normalized)?;
        let row = sqlx::query(
            r#"
            UPDATE submission_queue q
            SET status = $3,
                attempts = q.attempts + 1,
                next_attempt_at = CASE
                    WHEN $3 = 'deferred'
                        THEN NOW() + make_interval(secs => GREATEST(1, COALESCE($4, LEAST(3600, GREATEST(1, q.attempts + 1) * 300))))
                    ELSE NOW()
                END,
                last_error = CASE
                    WHEN $3 = 'relayed' THEN NULL
                    ELSE $5
                END,
                remote_message_ref = COALESCE($6, q.remote_message_ref),
                last_attempt_at = NOW(),
                terminal_at = CASE
                    WHEN $3 IN ('relayed', 'bounced', 'failed', 'cancelled')
                        THEN COALESCE(q.terminal_at, NOW())
                    ELSE q.terminal_at
                END,
                last_trace_id = $7,
                updated_at = NOW()
            WHERE q.tenant_id = $1 AND q.id = $2
            RETURNING
                q.status,
                q.last_trace_id,
                q.remote_message_ref,
                NULL::integer AS retry_after_seconds,
                NULL::text AS retry_policy,
                jsonb_build_object(
                    'status', q.status,
                    'traceId', q.last_trace_id,
                    'remoteMessageRef', q.remote_message_ref,
                    'lastError', q.last_error,
                    'transportResponse', $8::jsonb
                ) AS last_result_json
            "#,
        )
        .bind(&current.tenant_id)
        .bind(response.queue_id)
        .bind(&status_value)
        .bind(retry_after_seconds)
        .bind(normalized.detail.as_deref())
        .bind(normalized.remote_message_ref.as_deref())
        .bind(&normalized.trace_id)
        .bind(&technical_status)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("outbound queue item not found"))?;

        let message_id = current.message_id;
        let stored_status: String = row.try_get("status")?;
        let stored_trace_id: Option<String> = row.try_get("last_trace_id")?;
        let stored_remote_message_ref: Option<String> = row.try_get("remote_message_ref")?;
        let stored_retry_after_seconds: Option<i32> = row.try_get("retry_after_seconds")?;
        let stored_retry_policy: Option<String> = row.try_get("retry_policy")?;
        let stored_technical_status: Value = row.try_get("last_result_json")?;

        let mut tx = self.pool.begin().await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &current.tenant_id, current.account_id)
            .await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &current.tenant_id, current.account_id)
                .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &current.tenant_id,
            Some(current.account_id),
            None,
            "submission",
            response.queue_id,
            "updated",
            modseq,
            &principals,
            serde_json::json!({"messageId": message_id, "status": stored_status}),
        )
        .await?;
        Self::emit_mail_change(&mut tx, &current.tenant_id, current.account_id).await?;
        tx.commit().await?;

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
    fn same_trace_can_progress_from_deferred_to_relayed() {
        let response = response(
            TransportDeliveryStatus::Relayed,
            "trace-1",
            Some("remote-1"),
        );

        assert!(!should_ignore_handoff_response(
            "deferred",
            Some("trace-1"),
            None,
            &response
        ));
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
                retry_after_seconds: 60,
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
