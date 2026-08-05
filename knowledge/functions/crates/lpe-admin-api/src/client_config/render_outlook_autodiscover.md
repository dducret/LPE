---
type: Rust Function
title: render_outlook_autodiscover
resource: crates/lpe-admin-api/src/client_config.rs#L447-L533
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/render_ews_web_autodiscover_protocol
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exch_autodiscover_enabled
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/expr_autodiscover_enabled
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_http_autodiscover_selected
  - functions/crates/lpe-admin-api/src/client_config/render_exchange_provider_autodiscover_protocols
  - functions/crates/lpe-admin-api/src/client_config/render_mapi_http_autodiscover_protocol
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_get
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_does_not_publish_imap_or_smtp_without_explicit_edge_configuration
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
  - functions/crates/lpe-admin-api/src/client_config/tests/mapi_autodiscover_publication_is_env_opt_in
  - functions/crates/lpe-admin-api/src/client_config/tests/invalid_mapi_http_capability_header_is_ignored
  - functions/crates/lpe-admin-api/src/client_config/tests/mapi_http_capability_header_and_enable_flag_publish_mapi
  - functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_publication_has_separate_provider_opt_ins
  - functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_publication_works_with_ews_provider_opt_ins
  - functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_survives_mapi_capability_header_without_mapi_publication
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_ews_publication_is_env_opt_in
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_includes_required_pox_user_fields
---

# Signature

`fn render_outlook_autodiscover(config: &PublishedEndpoints, email: Option<&str>) -> String`

# Calls

- [render_ews_web_autodiscover_protocol](../../../../../functions/crates/lpe-admin-api/src/client_config/render_ews_web_autodiscover_protocol.md)
- [exch_autodiscover_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exch_autodiscover_enabled.md)
- [expr_autodiscover_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/expr_autodiscover_enabled.md)
- [mapi_http_autodiscover_selected](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_http_autodiscover_selected.md)
- [render_exchange_provider_autodiscover_protocols](../../../../../functions/crates/lpe-admin-api/src/client_config/render_exchange_provider_autodiscover_protocols.md)
- [render_mapi_http_autodiscover_protocol](../../../../../functions/crates/lpe-admin-api/src/client_config/render_mapi_http_autodiscover_protocol.md)

# Called by

- [outlook_autodiscover_get](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_get.md)
- [outlook_autodiscover_post](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post.md)
- [outlook_autodiscover_does_not_publish_imap_or_smtp_without_explicit_edge_configuration](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_does_not_publish_imap_or_smtp_without_explicit_edge_configuration.md)
- [outlook_autodiscover_publishes_imap_without_forcing_exchange_activesync](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_publishes_imap_without_forcing_exchange_activesync.md)
- [outlook_autodiscover_includes_smtp_only_when_explicitly_configured](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_includes_smtp_only_when_explicitly_configured.md)
- [outlook_autodiscover_can_publish_explicit_ews_web_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_explicit_ews_web_endpoint.md)
- [outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape.md)
- [outlook_autodiscover_can_publish_explicit_mapi_http_protocol](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_explicit_mapi_http_protocol.md)
- [outlook_autodiscover_mapi_probe_keeps_opt_in_ews_web_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_mapi_probe_keeps_opt_in_ews_web_endpoint.md)
- [outlook_autodiscover_mapi_http_capability_header_stays_env_gated](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_mapi_http_capability_header_stays_env_gated.md)
- [outlook_autodiscover_can_publish_exchange_provider_for_legacy_mapi_probe](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_exchange_provider_for_legacy_mapi_probe.md)
- [outlook_autodiscover_can_publish_exchange_providers_for_legacy_ews_probe](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_exchange_providers_for_legacy_ews_probe.md)
- [outlook_autodiscover_can_publish_legacy_exch_without_expr](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_legacy_exch_without_expr.md)
- [outlook_autodiscover_can_publish_legacy_expr_without_exch](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_can_publish_legacy_expr_without_exch.md)
- [outlook_autodiscover_expr_requires_rpc_proxy_publication](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_expr_requires_rpc_proxy_publication.md)
- [outlook_autodiscover_expr_requires_final_outlook_gate](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_expr_requires_final_outlook_gate.md)
- [mapi_enabled_does_not_hijack_default_outlook_imap_profile](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/mapi_enabled_does_not_hijack_default_outlook_imap_profile.md)
- [mapi_autodiscover_publication_is_env_opt_in](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/mapi_autodiscover_publication_is_env_opt_in.md)
- [invalid_mapi_http_capability_header_is_ignored](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/invalid_mapi_http_capability_header_is_ignored.md)
- [mapi_http_capability_header_and_enable_flag_publish_mapi](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/mapi_http_capability_header_and_enable_flag_publish_mapi.md)
- [legacy_exchange_autodiscover_publication_has_separate_provider_opt_ins](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_publication_has_separate_provider_opt_ins.md)
- [legacy_exchange_autodiscover_publication_works_with_ews_provider_opt_ins](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_publication_works_with_ews_provider_opt_ins.md)
- [legacy_exchange_autodiscover_survives_mapi_capability_header_without_mapi_publication](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_survives_mapi_capability_header_without_mapi_publication.md)
- [outlook_autodiscover_ews_publication_is_env_opt_in](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_ews_publication_is_env_opt_in.md)
- [outlook_autodiscover_includes_required_pox_user_fields](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_includes_required_pox_user_fields.md)