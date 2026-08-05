---
type: Rust Function
title: relay_message_to_target
resource: LPE-CT/src/smtp/outbound_delivery.rs#L393-L409
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message
---

# Signature

`async fn relay_message_to_target( target: &str, message: &QueuedMessage, route: &TransportRouteDecision, attempt_count: u32, ehlo_name: &str, ) -> Result<OutboundExecution>`

# Calls

- [relay_message_to_target_for_recipients](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients.md)

# Called by

- [relay_message](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message.md)