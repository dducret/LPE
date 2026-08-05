---
type: Rust Function
title: normalize_smtp_lookup_value
resource: crates/lpe-domain/src/normalization.rs#L98-L106
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value
  - functions/crates/lpe-exchange/src/service/ews/directory/normalize_address_book_lookup
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value
---

# Signature

`pub fn normalize_smtp_lookup_value(value: &str) -> String`

# Called by

- [normalize_nspi_lookup_value](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [normalize_address_book_lookup](../../../../../functions/crates/lpe-exchange/src/service/ews/directory/normalize_address_book_lookup.md)
- [rpc_proxy_normalize_nspi_lookup_value](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value.md)