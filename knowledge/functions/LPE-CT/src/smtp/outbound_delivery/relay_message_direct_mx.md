---
type: Rust Function
title: relay_message_direct_mx
resource: LPE-CT/src/smtp/outbound_delivery.rs#L130-L272
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/dsn/direct_mx_failure
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/policy/accepted_domain_is_verified
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/LPE-CT/src/smtp/outbound_delivery/deliver_outbound_to_local_recipients
  - functions/LPE-CT/src/smtp/outbound_delivery/direct_mx_targets
  - functions/LPE-CT/src/smtp/dsn/is_permanent_direct_mx_error
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message
---

# Signature

`async fn relay_message_direct_mx( config: &RuntimeConfig, message: &QueuedMessage, route: &TransportRouteDecision, attempt_count: u32, ) -> OutboundExecution`

# Calls

- [direct_mx_failure](../../../../../functions/LPE-CT/src/smtp/dsn/direct_mx_failure.md)
- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [accepted_domain_is_verified](../../../../../functions/LPE-CT/src/smtp/policy/accepted_domain_is_verified.md)
- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [deliver_outbound_to_local_recipients](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/deliver_outbound_to_local_recipients.md)
- [direct_mx_targets](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/direct_mx_targets.md)
- [is_permanent_direct_mx_error](../../../../../functions/LPE-CT/src/smtp/dsn/is_permanent_direct_mx_error.md)
- [relay_message_to_target_for_recipients](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients.md)

# Called by

- [relay_message](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message.md)