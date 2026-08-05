---
type: Rust Function
title: spawn_dummy_core
resource: LPE-CT/src/smtp/tests.rs#L3557-L3581
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core
  - functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery
  - functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx
  - functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api
  - functions/LPE-CT/src/smtp/tests/inbound_message_keeps_non_utf8_raw_bytes
---

# Signature

`async fn spawn_dummy_core(captured: Arc<Mutex<Option<InboundDeliveryRequest>>>) -> String`

# Called by

- [smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core.md)
- [smtp_ingress_marks_outlook_account_test_message](../../../../../functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message.md)
- [smtp_data_accepts_null_reverse_path_for_dsn_delivery](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery.md)
- [smtp_starttls_upgrades_to_tls_after_ready_reply](../../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply.md)
- [outbound_handoff_delivers_accepted_domain_locally_without_direct_mx](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx.md)
- [inbound_message_posts_to_core_delivery_api](../../../../../functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api.md)
- [inbound_message_keeps_non_utf8_raw_bytes](../../../../../functions/LPE-CT/src/smtp/tests/inbound_message_keeps_non_utf8_raw_bytes.md)