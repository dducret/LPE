---
type: Rust Function
title: web_external_protocol
resource: crates/lpe-admin-api/src/client_config.rs#L666-L685
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exch_autodiscover_enabled
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/expr_autodiscover_enabled
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/render_ews_web_autodiscover_protocol
---

# Signature

`fn web_external_protocol(config: &PublishedEndpoints) -> String`

# Calls

- [exch_autodiscover_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exch_autodiscover_enabled.md)
- [expr_autodiscover_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/expr_autodiscover_enabled.md)

# Called by

- [render_ews_web_autodiscover_protocol](../../../../../functions/crates/lpe-admin-api/src/client_config/render_ews_web_autodiscover_protocol.md)