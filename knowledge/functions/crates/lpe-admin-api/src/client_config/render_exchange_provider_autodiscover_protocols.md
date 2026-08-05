---
type: Rust Function
title: render_exchange_provider_autodiscover_protocols
resource: crates/lpe-admin-api/src/client_config.rs#L535-L596
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/mailbox_server_name
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exch_autodiscover_enabled
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/expr_autodiscover_enabled
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover
---

# Signature

`fn render_exchange_provider_autodiscover_protocols( config: &PublishedEndpoints, email: &str, ) -> String`

# Calls

- [mailbox_server_name](../../../../../functions/crates/lpe-admin-api/src/client_config/mailbox_server_name.md)
- [exch_autodiscover_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exch_autodiscover_enabled.md)
- [expr_autodiscover_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/expr_autodiscover_enabled.md)

# Called by

- [render_outlook_autodiscover](../../../../../functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover.md)