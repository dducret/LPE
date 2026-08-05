---
type: Rust Function
title: default_queue_for_status
resource: LPE-CT/src/smtp/outbound_policy.rs#L211-L220
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/dsn/direct_mx_failure
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients
---

# Signature

`pub(in crate::smtp) fn default_queue_for_status(status: &TransportDeliveryStatus) -> &'static str`

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [direct_mx_failure](../../../../../functions/LPE-CT/src/smtp/dsn/direct_mx_failure.md)
- [relay_message](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message.md)
- [relay_message_to_target_for_recipients](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients.md)