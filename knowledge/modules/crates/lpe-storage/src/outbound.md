---
type: Rust Module
title: outbound
resource: crates/lpe-storage/src/outbound.rs#L1-L582
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/lpe-domain-outboundmessagehandoffrequest-outboundmessagehandoffresponse-transportdeliverystatus-transportrecipient-transportretryadvice
  - external/serde-json-value
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-jmapemailrecipientrow-messagebccrecipientrow-outboundqueuestaterow-outboundqueuestatusupdate-pendingoutboundqueuerow-storage
  - external/lpe-domain-transportdeliverystatus-transporttechnicalstatus
  - external/super
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [queue_status_is_terminal](../../../../functions/crates/lpe-storage/src/outbound/queue_status_is_terminal.md)
- [same_trace_id](../../../../functions/crates/lpe-storage/src/outbound/same_trace_id.md)
- [is_duplicate_terminal_handoff](../../../../functions/crates/lpe-storage/src/outbound/is_duplicate_terminal_handoff.md)
- [would_regress_queue_status](../../../../functions/crates/lpe-storage/src/outbound/would_regress_queue_status.md)
- [should_ignore_handoff_response](../../../../functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response.md)
- [synthesized_retry_policy](../../../../functions/crates/lpe-storage/src/outbound/synthesized_retry_policy.md)
- [default_retry_after_seconds](../../../../functions/crates/lpe-storage/src/outbound/default_retry_after_seconds.md)
- [submission_queue_status](../../../../functions/crates/lpe-storage/src/outbound/submission_queue_status.md)
- [normalize_handoff_response](../../../../functions/crates/lpe-storage/src/outbound/normalize_handoff_response.md)
- [fetch_outbound_handoff_batch](../../../../functions/crates/lpe-storage/src/outbound/Storage/fetch_outbound_handoff_batch.md)
- [update_outbound_queue_status](../../../../functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status.md)
- [mark_outbound_queue_attempt_failure](../../../../functions/crates/lpe-storage/src/outbound/Storage/mark_outbound_queue_attempt_failure.md)
- [response](../../../../functions/crates/lpe-storage/src/outbound/response.md)
- [duplicate_handoff_is_recognized_by_trace_id_even_when_status_differs](../../../../functions/crates/lpe-storage/src/outbound/duplicate_handoff_is_recognized_by_trace_id_even_when_status_differs.md)
- [duplicate_terminal_handoff_is_recognized_by_remote_reference](../../../../functions/crates/lpe-storage/src/outbound/duplicate_terminal_handoff_is_recognized_by_remote_reference.md)
- [terminal_queue_states_do_not_regress](../../../../functions/crates/lpe-storage/src/outbound/terminal_queue_states_do_not_regress.md)
- [deferred_queue_state_does_not_regress_to_queued](../../../../functions/crates/lpe-storage/src/outbound/deferred_queue_state_does_not_regress_to_queued.md)
- [same_trace_can_progress_from_deferred_to_relayed](../../../../functions/crates/lpe-storage/src/outbound/same_trace_can_progress_from_deferred_to_relayed.md)
- [deferred_responses_without_retry_get_default_guidance](../../../../functions/crates/lpe-storage/src/outbound/deferred_responses_without_retry_get_default_guidance.md)
- [terminal_responses_clear_retry_guidance](../../../../functions/crates/lpe-storage/src/outbound/terminal_responses_clear_retry_guidance.md)

# Imports

- `anyhow::{anyhow, Result}`
- `lpe_domain::{
    OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, TransportDeliveryStatus,
    TransportRecipient, TransportRetryAdvice,
}`
- `serde_json::Value`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    JmapEmailRecipientRow, MessageBccRecipientRow, OutboundQueueStateRow,
    OutboundQueueStatusUpdate, PendingOutboundQueueRow, Storage,
}`
- `lpe_domain::{TransportDeliveryStatus, TransportTechnicalStatus}`
- `super::*`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)