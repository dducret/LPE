---
type: Rust Function
title: outbound_handoff_response_from_spool
resource: LPE-CT/src/smtp/outbound_policy.rs#L8-L32
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/outbound_policy/outbound_status_from_spool
  - functions/LPE-CT/src/smtp/outbound_policy/retry_advice_from_spooled_message
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
---

# Signature

`pub(in crate::smtp) fn outbound_handoff_response_from_spool( payload: &OutboundMessageHandoffRequest, trace_id: &str, queue: &str, message: QueuedMessage, ) -> OutboundMessageHandoffResponse`

# Calls

- [outbound_status_from_spool](../../../../../functions/LPE-CT/src/smtp/outbound_policy/outbound_status_from_spool.md)
- [retry_advice_from_spooled_message](../../../../../functions/LPE-CT/src/smtp/outbound_policy/retry_advice_from_spooled_message.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)