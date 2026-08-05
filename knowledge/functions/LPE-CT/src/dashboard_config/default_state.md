---
type: Rust Function
title: default_state
resource: LPE-CT/src/dashboard_config.rs#L744-L889
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name
  - functions/LPE-CT/src/dashboard_config/default_core_delivery_base_url
  - functions/LPE-CT/src/dashboard_config/parse_csv
  - functions/LPE-CT/src/dashboard_config/default_spool_queues
  - functions/LPE-CT/src/dashboard_config/default_policy_artifacts
  - functions/LPE-CT/src/dashboard_config/default_forbidden_canonical_data
  - functions/LPE-CT/src/dashboard_config/default_local_db_purposes
  - functions/LPE-CT/src/dashboard_config/default_local_db_listen_address
  - functions/LPE-CT/src/dashboard_config/default_local_db_network_scope
  - functions/LPE-CT/src/dashboard_config/default_local_db_notes
  - functions/LPE-CT/src/dashboard_config/default_antivirus_enabled
  - functions/LPE-CT/src/dashboard_config/default_antivirus_fail_closed
  - functions/LPE-CT/src/dashboard_config/default_antivirus_provider_chain
  - functions/LPE-CT/src/dashboard_config/default_bayespam_enabled
  - functions/LPE-CT/src/dashboard_config/default_bayespam_auto_learn
  - functions/LPE-CT/src/dashboard_config/default_bayespam_score_weight
  - functions/LPE-CT/src/dashboard_config/default_bayespam_min_token_length
  - functions/LPE-CT/src/dashboard_config/default_bayespam_max_tokens
  - functions/LPE-CT/src/dashboard_config/default_defer_on_auth_tempfail
  - functions/LPE-CT/src/dashboard_config/default_dnsbl_enabled
  - functions/LPE-CT/src/dashboard_config/default_dnsbl_zones
  - functions/LPE-CT/src/dashboard_config/default_reputation_enabled
  - functions/LPE-CT/src/dashboard_config/default_reputation_quarantine_threshold
  - functions/LPE-CT/src/dashboard_config/default_reputation_reject_threshold
  - functions/LPE-CT/src/dashboard_config/default_spam_quarantine_threshold
  - functions/LPE-CT/src/dashboard_config/default_spam_reject_threshold
  - functions/LPE-CT/src/dashboard_config/default_recipient_verification_settings
  - functions/LPE-CT/src/dashboard_config/default_dkim_settings
  - functions/LPE-CT/src/reporting/default_reporting_settings
  called_by:
  - functions/LPE-CT/src/load_or_initialize_state
  - functions/LPE-CT/src/dashboard_response_serializes_runtime_system_without_persisting_it
  - functions/LPE-CT/src/env_overrides_enable_private_local_db_profile
  - functions/LPE-CT/src/smtp/tests/plaintext_inbound_store
  - functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains
---

# Signature

`pub(crate) fn default_state() -> DashboardState`

# Calls

- [default_outbound_ehlo_name](../../../../functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name.md)
- [default_core_delivery_base_url](../../../../functions/LPE-CT/src/dashboard_config/default_core_delivery_base_url.md)
- [parse_csv](../../../../functions/LPE-CT/src/dashboard_config/parse_csv.md)
- [default_spool_queues](../../../../functions/LPE-CT/src/dashboard_config/default_spool_queues.md)
- [default_policy_artifacts](../../../../functions/LPE-CT/src/dashboard_config/default_policy_artifacts.md)
- [default_forbidden_canonical_data](../../../../functions/LPE-CT/src/dashboard_config/default_forbidden_canonical_data.md)
- [default_local_db_purposes](../../../../functions/LPE-CT/src/dashboard_config/default_local_db_purposes.md)
- [default_local_db_listen_address](../../../../functions/LPE-CT/src/dashboard_config/default_local_db_listen_address.md)
- [default_local_db_network_scope](../../../../functions/LPE-CT/src/dashboard_config/default_local_db_network_scope.md)
- [default_local_db_notes](../../../../functions/LPE-CT/src/dashboard_config/default_local_db_notes.md)
- [default_antivirus_enabled](../../../../functions/LPE-CT/src/dashboard_config/default_antivirus_enabled.md)
- [default_antivirus_fail_closed](../../../../functions/LPE-CT/src/dashboard_config/default_antivirus_fail_closed.md)
- [default_antivirus_provider_chain](../../../../functions/LPE-CT/src/dashboard_config/default_antivirus_provider_chain.md)
- [default_bayespam_enabled](../../../../functions/LPE-CT/src/dashboard_config/default_bayespam_enabled.md)
- [default_bayespam_auto_learn](../../../../functions/LPE-CT/src/dashboard_config/default_bayespam_auto_learn.md)
- [default_bayespam_score_weight](../../../../functions/LPE-CT/src/dashboard_config/default_bayespam_score_weight.md)
- [default_bayespam_min_token_length](../../../../functions/LPE-CT/src/dashboard_config/default_bayespam_min_token_length.md)
- [default_bayespam_max_tokens](../../../../functions/LPE-CT/src/dashboard_config/default_bayespam_max_tokens.md)
- [default_defer_on_auth_tempfail](../../../../functions/LPE-CT/src/dashboard_config/default_defer_on_auth_tempfail.md)
- [default_dnsbl_enabled](../../../../functions/LPE-CT/src/dashboard_config/default_dnsbl_enabled.md)
- [default_dnsbl_zones](../../../../functions/LPE-CT/src/dashboard_config/default_dnsbl_zones.md)
- [default_reputation_enabled](../../../../functions/LPE-CT/src/dashboard_config/default_reputation_enabled.md)
- [default_reputation_quarantine_threshold](../../../../functions/LPE-CT/src/dashboard_config/default_reputation_quarantine_threshold.md)
- [default_reputation_reject_threshold](../../../../functions/LPE-CT/src/dashboard_config/default_reputation_reject_threshold.md)
- [default_spam_quarantine_threshold](../../../../functions/LPE-CT/src/dashboard_config/default_spam_quarantine_threshold.md)
- [default_spam_reject_threshold](../../../../functions/LPE-CT/src/dashboard_config/default_spam_reject_threshold.md)
- [default_recipient_verification_settings](../../../../functions/LPE-CT/src/dashboard_config/default_recipient_verification_settings.md)
- [default_dkim_settings](../../../../functions/LPE-CT/src/dashboard_config/default_dkim_settings.md)
- [default_reporting_settings](../../../../functions/LPE-CT/src/reporting/default_reporting_settings.md)

# Called by

- [load_or_initialize_state](../../../../functions/LPE-CT/src/load_or_initialize_state.md)
- [dashboard_response_serializes_runtime_system_without_persisting_it](../../../../functions/LPE-CT/src/dashboard_response_serializes_runtime_system_without_persisting_it.md)
- [env_overrides_enable_private_local_db_profile](../../../../functions/LPE-CT/src/env_overrides_enable_private_local_db_profile.md)
- [plaintext_inbound_store](../../../../functions/LPE-CT/src/smtp/tests/plaintext_inbound_store.md)
- [runtime_store_with_accepted_domains](../../../../functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains.md)