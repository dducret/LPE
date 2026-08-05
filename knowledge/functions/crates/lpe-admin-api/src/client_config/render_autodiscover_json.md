---
type: Rust Function
title: render_autodiscover_json
resource: crates/lpe-admin-api/src/client_config.rs#L320-L347
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_autodiscover_enabled
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_json
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_defaults_to_pox_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_autodiscover_v1_returns_pox_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_supported_protocol_returns_protocol_and_url
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_ews_only_when_enabled
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_mapi_when_enabled
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_returns_activesync_only_for_mobile_protocol_probe
---

# Signature

`fn render_autodiscover_json( config: &PublishedEndpoints, protocol: Option<&str>, ) -> Option<Response>`

# Calls

- [mapi_autodiscover_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_autodiscover_enabled.md)
- [into_response](../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)

# Called by

- [outlook_autodiscover_json](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_json.md)
- [autodiscover_json_defaults_to_pox_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_defaults_to_pox_endpoint.md)
- [autodiscover_json_autodiscover_v1_returns_pox_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_autodiscover_v1_returns_pox_endpoint.md)
- [autodiscover_json_supported_protocol_returns_protocol_and_url](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_supported_protocol_returns_protocol_and_url.md)
- [autodiscover_json_publishes_ews_only_when_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_ews_only_when_enabled.md)
- [autodiscover_json_publishes_mapi_when_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_publishes_mapi_when_enabled.md)
- [autodiscover_json_returns_activesync_only_for_mobile_protocol_probe](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_returns_activesync_only_for_mobile_protocol_probe.md)