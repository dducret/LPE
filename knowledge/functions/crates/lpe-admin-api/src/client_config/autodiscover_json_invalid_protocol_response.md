---
type: Rust Function
title: autodiscover_json_invalid_protocol_response
resource: crates/lpe-admin-api/src/client_config.rs#L360-L382
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/supported_autodiscover_json_protocols
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_json
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_rest_without_fake_endpoint
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_jmap_protocol
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_unsupported_protocol_uses_microsoft_error_shape
---

# Signature

`fn autodiscover_json_invalid_protocol_response( config: &PublishedEndpoints, protocol: Option<&str>, ) -> Response`

# Calls

- [supported_autodiscover_json_protocols](../../../../../functions/crates/lpe-admin-api/src/client_config/supported_autodiscover_json_protocols.md)
- [into_response](../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)

# Called by

- [outlook_autodiscover_json](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_json.md)
- [autodiscover_json_rejects_rest_without_fake_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_rest_without_fake_endpoint.md)
- [autodiscover_json_rejects_jmap_protocol](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_rejects_jmap_protocol.md)
- [autodiscover_json_unsupported_protocol_uses_microsoft_error_shape](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_json_unsupported_protocol_uses_microsoft_error_shape.md)