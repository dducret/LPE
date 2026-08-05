---
type: Rust Function
title: sample_config
resource: crates/lpe-admin-api/src/client_config/tests.rs#L108-L134
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_can_publish_explicit_submission_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_defaults_to_pox_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_autodiscover_v1_returns_pox_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_supported_protocol_returns_protocol_and_url
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_rest_without_fake_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_jmap_protocol
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_unsupported_protocol_uses_microsoft_error_shape
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_ews_only_when_enabled
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_mapi_when_enabled
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_returns_activesync_only_for_mobile_protocol_probe
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_publishes_imap_without_forcing_exchange_activesync
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_includes_smtp_only_when_explicitly_configured
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_explicit_ews_web_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_explicit_mapi_http_protocol
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_mapi_probe_keeps_opt_in_ews_web_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_mapi_http_capability_header_stays_env_gated
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_exchange_provider_for_legacy_mapi_probe
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_exchange_providers_for_legacy_ews_probe
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_legacy_exch_without_expr
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_legacy_expr_without_exch
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_expr_requires_rpc_proxy_publication
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_expr_requires_final_outlook_gate
  - functions/crates/lpe-admin-api/src/client_config/tests/mapi_enabled_does_not_hijack_default_outlook_imap_profile
  - functions/crates/lpe-admin-api/src/client_config/tests/mobilesync_autodiscover_publishes_activesync_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_publishes_ews_user_settings_when_enabled
  - functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_is_not_published_for_default_outlook_imap_profile
  - functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_requires_separate_publication_opt_in
  - functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_reports_mapi_http_enabled_when_opted_in
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_includes_required_pox_user_fields
---

# Signature

`fn sample_config() -> PublishedEndpoints`

# Called by

- [thunderbird_autoconfig_can_publish_explicit_submission_endpoint](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_can_publish_explicit_submission_endpoint.md)
- [autodiscover_json_defaults_to_pox_endpoint](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_defaults_to_pox_endpoint.md)
- [autodiscover_json_autodiscover_v1_returns_pox_endpoint](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_autodiscover_v1_returns_pox_endpoint.md)
- [autodiscover_json_supported_protocol_returns_protocol_and_url](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_supported_protocol_returns_protocol_and_url.md)
- [autodiscover_json_rejects_rest_without_fake_endpoint](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_rest_without_fake_endpoint.md)
- [autodiscover_json_rejects_jmap_protocol](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_jmap_protocol.md)
- [autodiscover_json_unsupported_protocol_uses_microsoft_error_shape](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_unsupported_protocol_uses_microsoft_error_shape.md)
- [autodiscover_json_publishes_ews_only_when_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_ews_only_when_enabled.md)
- [autodiscover_json_publishes_mapi_when_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_mapi_when_enabled.md)
- [autodiscover_json_returns_activesync_only_for_mobile_protocol_probe](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_returns_activesync_only_for_mobile_protocol_probe.md)
- [outlook_autodiscover_publishes_imap_without_forcing_exchange_activesync](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_publishes_imap_without_forcing_exchange_activesync.md)
- [outlook_autodiscover_includes_smtp_only_when_explicitly_configured](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_includes_smtp_only_when_explicitly_configured.md)
- [outlook_autodiscover_can_publish_explicit_ews_web_endpoint](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_explicit_ews_web_endpoint.md)
- [outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape.md)
- [outlook_autodiscover_can_publish_explicit_mapi_http_protocol](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_explicit_mapi_http_protocol.md)
- [outlook_autodiscover_mapi_probe_keeps_opt_in_ews_web_endpoint](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_mapi_probe_keeps_opt_in_ews_web_endpoint.md)
- [outlook_autodiscover_mapi_http_capability_header_stays_env_gated](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_mapi_http_capability_header_stays_env_gated.md)
- [outlook_autodiscover_can_publish_exchange_provider_for_legacy_mapi_probe](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_exchange_provider_for_legacy_mapi_probe.md)
- [outlook_autodiscover_can_publish_exchange_providers_for_legacy_ews_probe](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_exchange_providers_for_legacy_ews_probe.md)
- [outlook_autodiscover_can_publish_legacy_exch_without_expr](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_legacy_exch_without_expr.md)
- [outlook_autodiscover_can_publish_legacy_expr_without_exch](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_legacy_expr_without_exch.md)
- [outlook_autodiscover_expr_requires_rpc_proxy_publication](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_expr_requires_rpc_proxy_publication.md)
- [outlook_autodiscover_expr_requires_final_outlook_gate](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_expr_requires_final_outlook_gate.md)
- [mapi_enabled_does_not_hijack_default_outlook_imap_profile](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/mapi_enabled_does_not_hijack_default_outlook_imap_profile.md)
- [mobilesync_autodiscover_publishes_activesync_endpoint](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/mobilesync_autodiscover_publishes_activesync_endpoint.md)
- [soap_autodiscover_publishes_ews_user_settings_when_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_publishes_ews_user_settings_when_enabled.md)
- [soap_autodiscover_is_not_published_for_default_outlook_imap_profile](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_is_not_published_for_default_outlook_imap_profile.md)
- [soap_autodiscover_requires_separate_publication_opt_in](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_requires_separate_publication_opt_in.md)
- [soap_autodiscover_reports_mapi_http_enabled_when_opted_in](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_reports_mapi_http_enabled_when_opted_in.md)
- [outlook_autodiscover_includes_required_pox_user_fields](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_includes_required_pox_user_fields.md)