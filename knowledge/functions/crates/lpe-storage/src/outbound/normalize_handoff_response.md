---
type: Rust Function
title: normalize_handoff_response
resource: crates/lpe-storage/src/outbound.rs#L80-L118
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/outbound/default_retry_after_seconds
  - functions/crates/lpe-storage/src/outbound/synthesized_retry_policy
  called_by:
  - functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status
  - functions/crates/lpe-storage/src/outbound/deferred_responses_without_retry_get_default_guidance
  - functions/crates/lpe-storage/src/outbound/terminal_responses_clear_retry_guidance
---

# Signature

`fn normalize_handoff_response( attempts: i32, response: &OutboundMessageHandoffResponse, ) -> OutboundMessageHandoffResponse`

# Calls

- [default_retry_after_seconds](../../../../../functions/crates/lpe-storage/src/outbound/default_retry_after_seconds.md)
- [synthesized_retry_policy](../../../../../functions/crates/lpe-storage/src/outbound/synthesized_retry_policy.md)

# Called by

- [update_outbound_queue_status](../../../../../functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status.md)
- [deferred_responses_without_retry_get_default_guidance](../../../../../functions/crates/lpe-storage/src/outbound/deferred_responses_without_retry_get_default_guidance.md)
- [terminal_responses_clear_retry_guidance](../../../../../functions/crates/lpe-storage/src/outbound/terminal_responses_clear_retry_guidance.md)