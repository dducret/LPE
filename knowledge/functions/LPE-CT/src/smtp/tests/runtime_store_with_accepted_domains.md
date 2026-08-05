---
type: Rust Function
title: runtime_store_with_accepted_domains
resource: LPE-CT/src/smtp/tests.rs#L237-L262
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/default_state
  called_by:
  - functions/LPE-CT/src/smtp/tests/smtp_mail_from_rejects_malformed_paths_unsupported_params_and_size_overflow
  - functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params
  - functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_enforces_transaction_recipient_limit
  - functions/LPE-CT/src/smtp/tests/smtp_long_command_line_returns_line_length_error
  - functions/LPE-CT/src/smtp/tests/smtp_command_sequence_requires_mail_and_recipient_before_data
  - functions/LPE-CT/src/smtp/tests/smtp_rcpt_accepts_configured_domain_and_rejects_external_relay_domain
  - functions/LPE-CT/src/smtp/tests/smtp_null_reverse_path_is_controlled_per_recipient_domain
  - functions/LPE-CT/src/smtp/tests/smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain
  - functions/LPE-CT/src/smtp/tests/smtp_ehlo_advertises_starttls_when_tls_is_available
  - functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_without_tls_config
  - functions/LPE-CT/src/smtp/tests/smtp_public_ingress_does_not_advertise_or_accept_auth
  - functions/LPE-CT/src/smtp/tests/smtp_starttls_requires_ehlo_or_helo_first
  - functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_after_tls_upgrade
  - functions/LPE-CT/src/smtp/tests/smtp_session_rejects_when_ha_role_is_standby
---

# Signature

`fn runtime_store_with_accepted_domains( domains: &[(&str, bool)], ) -> Arc<Mutex<crate::DashboardState>>`

# Calls

- [default_state](../../../../../functions/LPE-CT/src/dashboard_config/default_state.md)

# Called by

- [smtp_mail_from_rejects_malformed_paths_unsupported_params_and_size_overflow](../../../../../functions/LPE-CT/src/smtp/tests/smtp_mail_from_rejects_malformed_paths_unsupported_params_and_size_overflow.md)
- [smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params](../../../../../functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params.md)
- [smtp_rcpt_to_enforces_transaction_recipient_limit](../../../../../functions/LPE-CT/src/smtp/tests/smtp_rcpt_to_enforces_transaction_recipient_limit.md)
- [smtp_long_command_line_returns_line_length_error](../../../../../functions/LPE-CT/src/smtp/tests/smtp_long_command_line_returns_line_length_error.md)
- [smtp_command_sequence_requires_mail_and_recipient_before_data](../../../../../functions/LPE-CT/src/smtp/tests/smtp_command_sequence_requires_mail_and_recipient_before_data.md)
- [smtp_rcpt_accepts_configured_domain_and_rejects_external_relay_domain](../../../../../functions/LPE-CT/src/smtp/tests/smtp_rcpt_accepts_configured_domain_and_rejects_external_relay_domain.md)
- [smtp_null_reverse_path_is_controlled_per_recipient_domain](../../../../../functions/LPE-CT/src/smtp/tests/smtp_null_reverse_path_is_controlled_per_recipient_domain.md)
- [smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain](../../../../../functions/LPE-CT/src/smtp/tests/smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain.md)
- [smtp_ehlo_advertises_starttls_when_tls_is_available](../../../../../functions/LPE-CT/src/smtp/tests/smtp_ehlo_advertises_starttls_when_tls_is_available.md)
- [smtp_ehlo_does_not_advertise_starttls_without_tls_config](../../../../../functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_without_tls_config.md)
- [smtp_public_ingress_does_not_advertise_or_accept_auth](../../../../../functions/LPE-CT/src/smtp/tests/smtp_public_ingress_does_not_advertise_or_accept_auth.md)
- [smtp_starttls_requires_ehlo_or_helo_first](../../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_requires_ehlo_or_helo_first.md)
- [smtp_ehlo_does_not_advertise_starttls_after_tls_upgrade](../../../../../functions/LPE-CT/src/smtp/tests/smtp_ehlo_does_not_advertise_starttls_after_tls_upgrade.md)
- [smtp_session_rejects_when_ha_role_is_standby](../../../../../functions/LPE-CT/src/smtp/tests/smtp_session_rejects_when_ha_role_is_standby.md)