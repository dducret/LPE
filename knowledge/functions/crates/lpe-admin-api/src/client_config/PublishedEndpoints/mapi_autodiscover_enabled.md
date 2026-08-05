---
type: Rust Method
title: mapi_autodiscover_enabled
resource: crates/lpe-admin-api/src/client_config.rs#L296-L298
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exchange_autodiscover_enabled
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_http_autodiscover_selected
  - functions/crates/lpe-admin-api/src/client_config/render_autodiscover_json
  - functions/crates/lpe-admin-api/src/client_config/supported_autodiscover_json_protocols
---

# Signature

`fn mapi_autodiscover_enabled(&self) -> bool`

# Called by

- [exchange_autodiscover_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exchange_autodiscover_enabled.md)
- [mapi_http_autodiscover_selected](../../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_http_autodiscover_selected.md)
- [render_autodiscover_json](../../../../../../functions/crates/lpe-admin-api/src/client_config/render_autodiscover_json.md)
- [supported_autodiscover_json_protocols](../../../../../../functions/crates/lpe-admin-api/src/client_config/supported_autodiscover_json_protocols.md)