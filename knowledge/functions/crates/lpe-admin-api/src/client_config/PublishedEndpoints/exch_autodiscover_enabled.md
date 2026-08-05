---
type: Rust Method
title: exch_autodiscover_enabled
resource: crates/lpe-admin-api/src/client_config.rs#L304-L306
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exchange_autodiscover_enabled
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover
  - functions/crates/lpe-admin-api/src/client_config/render_exchange_provider_autodiscover_protocols
  - functions/crates/lpe-admin-api/src/client_config/web_external_protocol
---

# Signature

`fn exch_autodiscover_enabled(&self) -> bool`

# Calls

- [exchange_autodiscover_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exchange_autodiscover_enabled.md)

# Called by

- [render_outlook_autodiscover](../../../../../../functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover.md)
- [render_exchange_provider_autodiscover_protocols](../../../../../../functions/crates/lpe-admin-api/src/client_config/render_exchange_provider_autodiscover_protocols.md)
- [web_external_protocol](../../../../../../functions/crates/lpe-admin-api/src/client_config/web_external_protocol.md)