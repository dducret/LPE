---
type: Rust Function
title: retry_advice_from_spooled_message
resource: LPE-CT/src/smtp/outbound_policy.rs#L53-L73
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/retry_after_seconds
  called_by:
  - functions/LPE-CT/src/smtp/outbound_policy/outbound_handoff_response_from_spool
---

# Signature

`fn retry_advice_from_spooled_message( payload: &OutboundMessageHandoffRequest, message: &QueuedMessage, ) -> TransportRetryAdvice`

# Calls

- [retry_after_seconds](../../../../../functions/LPE-CT/src/smtp/retry_after_seconds.md)

# Called by

- [outbound_handoff_response_from_spool](../../../../../functions/LPE-CT/src/smtp/outbound_policy/outbound_handoff_response_from_spool.md)