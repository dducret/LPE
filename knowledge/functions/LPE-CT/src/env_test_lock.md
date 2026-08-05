---
type: Rust Function
title: env_test_lock
resource: LPE-CT/src/main.rs#L108-L112
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dkim_signing/dkim_signer_adds_header_when_domain_key_exists
  - functions/LPE-CT/src/dkim_signing/dkim_signer_prefers_from_domain_before_sender_domain
  - functions/LPE-CT/src/integration_secret_must_be_present_and_strong
  - functions/LPE-CT/src/ha_role_check_accepts_only_active_role
  - functions/LPE-CT/src/ha_non_active_gate_reports_non_active_roles
  - functions/LPE-CT/src/env_overrides_enable_private_local_db_profile
  - functions/LPE-CT/src/submission_listener_requires_bind_and_tls_material
  - functions/LPE-CT/src/signed_integration_requests_reject_replay
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core
  - functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message
  - functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery
  - functions/LPE-CT/src/smtp/tests/smtp_data_defers_with_trace_when_core_delivery_is_unavailable
  - functions/LPE-CT/src/smtp/tests/inbound_bridge_failure_keeps_deferred_custody_with_audit
  - functions/LPE-CT/src/smtp/tests/smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce
  - functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply
  - functions/LPE-CT/src/smtp/tests/smtp_session_rejects_when_ha_role_is_standby
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx
  - functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api
  - functions/LPE-CT/src/smtp/tests/inbound_message_keeps_non_utf8_raw_bytes
  - functions/LPE-CT/src/submission/submit_message_posts_trace_header_and_returns_success
  - functions/LPE-CT/src/submission/submit_message_rejects_non_accepted_success_body_before_smtp_final_reply
  - functions/LPE-CT/src/transport_policy/address_policy_supports_exact_and_domain_rules
  - functions/LPE-CT/src/transport_policy/attachment_policy_checks_extension_and_detected_type
  - functions/LPE-CT/src/transport_policy/attachment_policy_normalizes_leading_dot_extensions
  - functions/LPE-CT/src/transport_policy/recipient_verification_uses_internal_api
---

# Signature

`pub(crate) fn env_test_lock() -> std::sync::MutexGuard<'static, ()>`

# Called by

- [dkim_signer_adds_header_when_domain_key_exists](../../../functions/LPE-CT/src/dkim_signing/dkim_signer_adds_header_when_domain_key_exists.md)
- [dkim_signer_prefers_from_domain_before_sender_domain](../../../functions/LPE-CT/src/dkim_signing/dkim_signer_prefers_from_domain_before_sender_domain.md)
- [integration_secret_must_be_present_and_strong](../../../functions/LPE-CT/src/integration_secret_must_be_present_and_strong.md)
- [ha_role_check_accepts_only_active_role](../../../functions/LPE-CT/src/ha_role_check_accepts_only_active_role.md)
- [ha_non_active_gate_reports_non_active_roles](../../../functions/LPE-CT/src/ha_non_active_gate_reports_non_active_roles.md)
- [env_overrides_enable_private_local_db_profile](../../../functions/LPE-CT/src/env_overrides_enable_private_local_db_profile.md)
- [submission_listener_requires_bind_and_tls_material](../../../functions/LPE-CT/src/submission_listener_requires_bind_and_tls_material.md)
- [signed_integration_requests_reject_replay](../../../functions/LPE-CT/src/signed_integration_requests_reject_replay.md)
- [smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core](../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core.md)
- [smtp_ingress_marks_outlook_account_test_message](../../../functions/LPE-CT/src/smtp/tests/smtp_ingress_marks_outlook_account_test_message.md)
- [inbound_delivery_keeps_durable_spool_custody_until_core_accepts](../../../functions/LPE-CT/src/smtp/tests/inbound_delivery_keeps_durable_spool_custody_until_core_accepts.md)
- [smtp_data_accepts_null_reverse_path_for_dsn_delivery](../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery.md)
- [smtp_data_defers_with_trace_when_core_delivery_is_unavailable](../../../functions/LPE-CT/src/smtp/tests/smtp_data_defers_with_trace_when_core_delivery_is_unavailable.md)
- [inbound_bridge_failure_keeps_deferred_custody_with_audit](../../../functions/LPE-CT/src/smtp/tests/inbound_bridge_failure_keeps_deferred_custody_with_audit.md)
- [smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce](../../../functions/LPE-CT/src/smtp/tests/smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce.md)
- [smtp_starttls_upgrades_to_tls_after_ready_reply](../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply.md)
- [smtp_session_rejects_when_ha_role_is_standby](../../../functions/LPE-CT/src/smtp/tests/smtp_session_rejects_when_ha_role_is_standby.md)
- [outbound_handoff_delivers_accepted_domain_locally_without_direct_mx](../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_delivers_accepted_domain_locally_without_direct_mx.md)
- [inbound_message_posts_to_core_delivery_api](../../../functions/LPE-CT/src/smtp/tests/inbound_message_posts_to_core_delivery_api.md)
- [inbound_message_keeps_non_utf8_raw_bytes](../../../functions/LPE-CT/src/smtp/tests/inbound_message_keeps_non_utf8_raw_bytes.md)
- [submit_message_posts_trace_header_and_returns_success](../../../functions/LPE-CT/src/submission/submit_message_posts_trace_header_and_returns_success.md)
- [submit_message_rejects_non_accepted_success_body_before_smtp_final_reply](../../../functions/LPE-CT/src/submission/submit_message_rejects_non_accepted_success_body_before_smtp_final_reply.md)
- [address_policy_supports_exact_and_domain_rules](../../../functions/LPE-CT/src/transport_policy/address_policy_supports_exact_and_domain_rules.md)
- [attachment_policy_checks_extension_and_detected_type](../../../functions/LPE-CT/src/transport_policy/attachment_policy_checks_extension_and_detected_type.md)
- [attachment_policy_normalizes_leading_dot_extensions](../../../functions/LPE-CT/src/transport_policy/attachment_policy_normalizes_leading_dot_extensions.md)
- [recipient_verification_uses_internal_api](../../../functions/LPE-CT/src/transport_policy/recipient_verification_uses_internal_api.md)