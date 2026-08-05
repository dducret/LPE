---
type: Rust Function
title: deliver_outbound_to_local_recipients
resource: LPE-CT/src/smtp/outbound_delivery.rs#L274-L343
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/retry_after_seconds
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx
---

# Signature

`async fn deliver_outbound_to_local_recipients( config: &RuntimeConfig, message: &QueuedMessage, route: &TransportRouteDecision, attempt_count: u32, recipients: &[String], ) -> OutboundExecution`

# Calls

- [retry_after_seconds](../../../../../functions/LPE-CT/src/smtp/retry_after_seconds.md)

# Called by

- [relay_message_direct_mx](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx.md)