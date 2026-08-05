---
type: Rust Function
title: relay_message
resource: LPE-CT/src/smtp/outbound_delivery.rs#L3-L128
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target
  - functions/LPE-CT/src/smtp/dsn/is_permanent_relay_error
  - functions/LPE-CT/src/smtp/retry_after_seconds
  - functions/LPE-CT/src/smtp/outbound_policy/default_queue_for_status
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
---

# Signature

`pub(in crate::smtp) async fn relay_message( config: &RuntimeConfig, message: &QueuedMessage, route: &TransportRouteDecision, attempt_count: u32, _last_attempt_error: Option<&str>, ) -> OutboundExecution`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [relay_message_direct_mx](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx.md)
- [relay_message_to_target](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target.md)
- [is_permanent_relay_error](../../../../../functions/LPE-CT/src/smtp/dsn/is_permanent_relay_error.md)
- [retry_after_seconds](../../../../../functions/LPE-CT/src/smtp/retry_after_seconds.md)
- [default_queue_for_status](../../../../../functions/LPE-CT/src/smtp/outbound_policy/default_queue_for_status.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)