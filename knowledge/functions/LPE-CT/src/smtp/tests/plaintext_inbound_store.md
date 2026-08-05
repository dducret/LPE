---
type: Rust Function
title: plaintext_inbound_store
resource: LPE-CT/src/smtp/tests.rs#L207-L235
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/default_state
  called_by:
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core
  - functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery
  - functions/LPE-CT/src/smtp/tests/smtp_data_defers_with_trace_when_core_delivery_is_unavailable
  - functions/LPE-CT/src/smtp/tests/smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce
  - functions/LPE-CT/src/smtp/tests/smtp_data_rejects_with_policy_reason_and_trace
  - functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply
---

# Signature

`fn plaintext_inbound_store(core_delivery_base_url: String) -> Arc<Mutex<crate::DashboardState>>`

# Calls

- [default_state](../../../../../functions/LPE-CT/src/dashboard_config/default_state.md)

# Called by

- [smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_plaintext_for_local_domain_and_hands_to_core.md)
- [smtp_data_accepts_null_reverse_path_for_dsn_delivery](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_accepts_null_reverse_path_for_dsn_delivery.md)
- [smtp_data_defers_with_trace_when_core_delivery_is_unavailable](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_defers_with_trace_when_core_delivery_is_unavailable.md)
- [smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce](../../../../../functions/LPE-CT/src/smtp/tests/smtp_unknown_local_recipient_core_rejection_defers_without_backscatter_bounce.md)
- [smtp_data_rejects_with_policy_reason_and_trace](../../../../../functions/LPE-CT/src/smtp/tests/smtp_data_rejects_with_policy_reason_and_trace.md)
- [smtp_starttls_upgrades_to_tls_after_ready_reply](../../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply.md)