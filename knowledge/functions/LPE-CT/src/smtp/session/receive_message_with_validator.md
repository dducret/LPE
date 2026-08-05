---
type: Rust Function
title: receive_message_with_validator
resource: LPE-CT/src/smtp/session.rs#L399-L689
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/queue_store/persist_message
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/outlook_test_message/classify_smtp_message
  - functions/LPE-CT/src/observability/record_outlook_test_message
  - functions/LPE-CT/src/smtp/queue_store/move_message
  - functions/LPE-CT/src/smtp/audit/append_transport_audit
  - functions/LPE-CT/src/smtp/antivirus/classify_inbound_message
  - functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata_or_warn
  - functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
  - functions/LPE-CT/src/smtp/parse_peer_ip
  - functions/LPE-CT/src/smtp/inbound_policy/apply_filter_verdict
  - functions/LPE-CT/src/smtp/reputation/update_reputation
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
  - functions/LPE-CT/src/observability/record_smtp_session
  called_by:
  - functions/LPE-CT/src/smtp/session/receive_message
  - functions/LPE-CT/src/smtp/tests/inbound_magika_failure_is_quarantined
  - functions/LPE-CT/src/smtp/tests/inbound_message_keeps_non_utf8_raw_bytes
---

# Signature

`pub(in crate::smtp) async fn receive_message_with_validator<D: Detector>( validator: &Validator<D>, spool_dir: &Path, config: &RuntimeConfig, peer: String, helo: String, mail_from: String, rcpt_to: Vec<String>, data: Vec<u8>, ) -> Result<QueuedMessage>`

# Calls

- [persist_message](../../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [classify_smtp_message](../../../../../functions/LPE-CT/src/outlook_test_message/classify_smtp_message.md)
- [record_outlook_test_message](../../../../../functions/LPE-CT/src/observability/record_outlook_test_message.md)
- [move_message](../../../../../functions/LPE-CT/src/smtp/queue_store/move_message.md)
- [append_transport_audit](../../../../../functions/LPE-CT/src/smtp/audit/append_transport_audit.md)
- [classify_inbound_message](../../../../../functions/LPE-CT/src/smtp/antivirus/classify_inbound_message.md)
- [persist_quarantine_metadata_or_warn](../../../../../functions/LPE-CT/src/smtp/quarantine/persist_quarantine_metadata_or_warn.md)
- [evaluate_attachment_policy_with_config](../../../../../functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config.md)
- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)
- [parse_peer_ip](../../../../../functions/LPE-CT/src/smtp/parse_peer_ip.md)
- [apply_filter_verdict](../../../../../functions/LPE-CT/src/smtp/inbound_policy/apply_filter_verdict.md)
- [update_reputation](../../../../../functions/LPE-CT/src/smtp/reputation/update_reputation.md)
- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)
- [record_smtp_session](../../../../../functions/LPE-CT/src/observability/record_smtp_session.md)

# Called by

- [receive_message](../../../../../functions/LPE-CT/src/smtp/session/receive_message.md)
- [inbound_magika_failure_is_quarantined](../../../../../functions/LPE-CT/src/smtp/tests/inbound_magika_failure_is_quarantined.md)
- [inbound_message_keeps_non_utf8_raw_bytes](../../../../../functions/LPE-CT/src/smtp/tests/inbound_message_keeps_non_utf8_raw_bytes.md)