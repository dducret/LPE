---
type: Rust Function
title: receive_message
resource: LPE-CT/src/smtp/session.rs#L377-L397
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message
  - functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts
  - functions/LPE-CT/src/smtp/tests/inbound_bridge_failure_keeps_deferred_custody_with_audit
  - functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api
---

# Signature

`pub(in crate::smtp) async fn receive_message( spool_dir: &Path, config: &RuntimeConfig, peer: String, helo: String, mail_from: String, rcpt_to: Vec<String>, data: Vec<u8>, ) -> Result<QueuedMessage>`

# Calls

- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)

# Called by

- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [smtp_ingress_marks_outlook_account_test_message](../../../../../functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message.md)
- [inbound_delivery_keeps_durable_spool_custody_until_core_accepts](../../../../../functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts.md)
- [inbound_bridge_failure_keeps_deferred_custody_with_audit](../../../../../functions/LPE-CT/src/smtp/tests/inbound_bridge_failure_keeps_deferred_custody_with_audit.md)
- [inbound_message_posts_to_core_delivery_api](../../../../../functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api.md)