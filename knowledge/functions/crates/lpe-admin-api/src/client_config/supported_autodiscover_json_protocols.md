---
type: Rust Function
title: supported_autodiscover_json_protocols
resource: crates/lpe-admin-api/src/client_config.rs#L349-L358
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_autodiscover_enabled
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/autodiscover_json_invalid_protocol_response
---

# Signature

`fn supported_autodiscover_json_protocols(config: &PublishedEndpoints) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [mapi_autodiscover_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_autodiscover_enabled.md)

# Called by

- [autodiscover_json_invalid_protocol_response](../../../../../functions/crates/lpe-admin-api/src/client_config/autodiscover_json_invalid_protocol_response.md)