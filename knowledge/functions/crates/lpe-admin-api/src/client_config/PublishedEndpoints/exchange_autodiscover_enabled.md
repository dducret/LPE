---
type: Rust Method
title: exchange_autodiscover_enabled
resource: crates/lpe-admin-api/src/client_config.rs#L292-L294
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_autodiscover_enabled
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exch_autodiscover_enabled
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/expr_autodiscover_enabled
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/soap_exchange_autodiscover_enabled
---

# Signature

`fn exchange_autodiscover_enabled(&self) -> bool`

# Calls

- [mapi_autodiscover_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/mapi_autodiscover_enabled.md)

# Called by

- [exch_autodiscover_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/exch_autodiscover_enabled.md)
- [expr_autodiscover_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/expr_autodiscover_enabled.md)
- [soap_exchange_autodiscover_enabled](../../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/soap_exchange_autodiscover_enabled.md)