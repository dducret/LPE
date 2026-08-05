---
type: Rust Function
title: relay_message_to_target_for_recipients
resource: LPE-CT/src/smtp/outbound_delivery.rs#L411-L620
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/normalize_smtp_target
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/LPE-CT/src/smtp/protocol/expect_smtp
  - functions/LPE-CT/src/smtp/protocol/smtp_command_reply
  - functions/LPE-CT/src/smtp/dsn/parse_enhanced_status
  - functions/LPE-CT/src/smtp/retry_after_seconds
  - functions/LPE-CT/src/smtp/outbound_policy/default_queue_for_status
  - functions/LPE-CT/src/smtp/protocol/read_smtp_reply
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target
---

# Signature

`async fn relay_message_to_target_for_recipients( target: &str, message: &QueuedMessage, route: &TransportRouteDecision, attempt_count: u32, recipients: &[String], ehlo_name: &str, ) -> Result<OutboundExecution>`

# Calls

- [normalize_smtp_target](../../../../../functions/LPE-CT/src/smtp/normalize_smtp_target.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [expect_smtp](../../../../../functions/LPE-CT/src/smtp/protocol/expect_smtp.md)
- [smtp_command_reply](../../../../../functions/LPE-CT/src/smtp/protocol/smtp_command_reply.md)
- [parse_enhanced_status](../../../../../functions/LPE-CT/src/smtp/dsn/parse_enhanced_status.md)
- [retry_after_seconds](../../../../../functions/LPE-CT/src/smtp/retry_after_seconds.md)
- [default_queue_for_status](../../../../../functions/LPE-CT/src/smtp/outbound_policy/default_queue_for_status.md)
- [read_smtp_reply](../../../../../functions/LPE-CT/src/smtp/protocol/read_smtp_reply.md)

# Called by

- [relay_message_direct_mx](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx.md)
- [relay_message_to_target](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target.md)