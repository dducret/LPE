---
type: Rust Module
title: tests
resource: crates/lpe-admin-api/src/client_config/tests.rs#L1-L1194
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-autodiscover-json-invalid-protocol-response-jmap-well-known-location-outlook-autodiscover-json-parse-autodiscover-email-render-autodiscover-json-render-mobilesync-autodiscover-render-outlook-autodiscover-render-soap-user-settings-autodiscover-render-soap-user-settings-response-render-thunderbird-autoconfig-requested-mobilesync-schema-requested-soap-user-settings-autodiscoverjsonquery-publishedendpoints
  - external/axum-body-extract-path-extract-query-http-headermap-http-uri
  - external/quick-xml-events-event-reader
  - external/std-sync-mutex
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [XmlNode](../../../../../classes/crates/lpe-admin-api/src/client_config/tests/XmlNode.md)
- [child](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/XmlNode/child.md)
- [child_text](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/XmlNode/child_text.md)
- [children_named](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/XmlNode/children_named.md)
- [parse_xml](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/parse_xml.md)
- [local_name](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/local_name.md)
- [outlook_account](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_account.md)
- [web_protocol](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/web_protocol.md)
- [sample_config](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/sample_config.md)
- [thunderbird_autoconfig_publishes_imap_only_when_edge_imaps_is_configured](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_publishes_imap_only_when_edge_imaps_is_configured.md)
- [thunderbird_autoconfig_can_publish_explicit_submission_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_can_publish_explicit_submission_endpoint.md)
- [outlook_autodiscover_does_not_publish_imap_or_smtp_without_explicit_edge_configuration](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_does_not_publish_imap_or_smtp_without_explicit_edge_configuration.md)
- [jmap_well_known_redirects_to_public_session_url](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/jmap_well_known_redirects_to_public_session_url.md)
- [autodiscover_json_defaults_to_pox_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_defaults_to_pox_endpoint.md)
- [autodiscover_json_autodiscover_v1_returns_pox_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_autodiscover_v1_returns_pox_endpoint.md)
- [autodiscover_json_supported_protocol_returns_protocol_and_url](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_supported_protocol_returns_protocol_and_url.md)
- [autodiscover_json_accepts_outlook_redirect_count_request](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_accepts_outlook_redirect_count_request.md)
- [autodiscover_json_rejects_rest_without_fake_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_rest_without_fake_endpoint.md)
- [autodiscover_json_rejects_jmap_protocol](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_jmap_protocol.md)
- [autodiscover_json_handler_rejects_rest_request_with_redirect_count](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_handler_rejects_rest_request_with_redirect_count.md)
- [autodiscover_json_unsupported_protocol_uses_microsoft_error_shape](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_unsupported_protocol_uses_microsoft_error_shape.md)
- [autodiscover_json_publishes_ews_only_when_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_ews_only_when_enabled.md)
- [autodiscover_json_publishes_mapi_when_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_mapi_when_enabled.md)
- [autodiscover_json_returns_activesync_only_for_mobile_protocol_probe](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_returns_activesync_only_for_mobile_protocol_probe.md)
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
- [autodiscover_request_parser_extracts_email_address](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_request_parser_extracts_email_address.md)
- [autodiscover_request_parser_extracts_namespaced_email_address](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_request_parser_extracts_namespaced_email_address.md)
- [autodiscover_request_parser_extracts_soap_mailbox](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_request_parser_extracts_soap_mailbox.md)
- [autodiscover_detects_mobilesync_response_schema_request](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_detects_mobilesync_response_schema_request.md)
- [mobilesync_autodiscover_publishes_activesync_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/mobilesync_autodiscover_publishes_activesync_endpoint.md)
- [soap_autodiscover_publishes_ews_user_settings_when_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_publishes_ews_user_settings_when_enabled.md)
- [soap_autodiscover_is_not_published_for_default_outlook_imap_profile](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_is_not_published_for_default_outlook_imap_profile.md)
- [soap_autodiscover_requires_separate_publication_opt_in](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_requires_separate_publication_opt_in.md)
- [soap_exchange_autodiscover_publication_is_env_opt_in](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_exchange_autodiscover_publication_is_env_opt_in.md)
- [soap_autodiscover_reports_mapi_http_enabled_when_opted_in](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_reports_mapi_http_enabled_when_opted_in.md)
- [autodiscover_detects_soap_get_user_settings_request](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_detects_soap_get_user_settings_request.md)
- [outlook_autodiscover_includes_required_pox_user_fields](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_includes_required_pox_user_fields.md)

# Imports

- `super::{
    autodiscover_json_invalid_protocol_response, jmap_well_known_location,
    outlook_autodiscover_json, parse_autodiscover_email, render_autodiscover_json,
    render_mobilesync_autodiscover, render_outlook_autodiscover,
    render_soap_user_settings_autodiscover, render_soap_user_settings_response,
    render_thunderbird_autoconfig, requested_mobilesync_schema, requested_soap_user_settings,
    AutodiscoverJsonQuery, PublishedEndpoints,
}`
- `axum::{body, extract::Path, extract::Query, http::HeaderMap, http::Uri}`
- `quick_xml::{events::Event, Reader}`
- `std::sync::Mutex`

# Member of

- [lpe-admin-api](../../../../../packages/crates/lpe-admin-api.md)