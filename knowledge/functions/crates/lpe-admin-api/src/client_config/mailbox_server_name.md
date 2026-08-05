---
type: Rust Function
title: mailbox_server_name
resource: crates/lpe-admin-api/src/client_config.rs#L598-L600
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/ews_host
  - functions/crates/lpe-admin-api/src/client_config/fallback_host
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/render_exchange_provider_autodiscover_protocols
---

# Signature

`fn mailbox_server_name(config: &PublishedEndpoints) -> &str`

# Calls

- [ews_host](../../../../../functions/crates/lpe-admin-api/src/client_config/ews_host.md)
- [fallback_host](../../../../../functions/crates/lpe-admin-api/src/client_config/fallback_host.md)

# Called by

- [render_exchange_provider_autodiscover_protocols](../../../../../functions/crates/lpe-admin-api/src/client_config/render_exchange_provider_autodiscover_protocols.md)