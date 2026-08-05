---
type: Rust Method
title: mapi_http_autodiscover_selected
resource: crates/lpe-admin-api/src/client_config.rs#L300-L302
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_autodiscover_enabled
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover
---

# Signature

`fn mapi_http_autodiscover_selected(&self) -> bool`

# Calls

- [mapi_autodiscover_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_autodiscover_enabled.md)

# Called by

- [render_outlook_autodiscover](../../../../../../functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover.md)