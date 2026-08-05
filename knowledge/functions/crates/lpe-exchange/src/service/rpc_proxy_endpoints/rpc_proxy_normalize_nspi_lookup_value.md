---
type: Rust Function
title: rpc_proxy_normalize_nspi_lookup_value
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1176-L1178
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/normalization/normalize_smtp_lookup_value
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_principal_matches
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_exact_match
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_matches
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_ascii_lookup_values
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_utf16_lookup_values
---

# Signature

`fn rpc_proxy_normalize_nspi_lookup_value(value: &str) -> String`

# Calls

- [normalize_smtp_lookup_value](../../../../../../functions/crates/lpe-domain/src/normalization/normalize_smtp_lookup_value.md)

# Called by

- [rpc_proxy_nspi_principal_matches](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_principal_matches.md)
- [rpc_proxy_nspi_entry_exact_match](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_exact_match.md)
- [rpc_proxy_nspi_entry_matches](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_matches.md)
- [rpc_proxy_nspi_ascii_lookup_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_ascii_lookup_values.md)
- [rpc_proxy_nspi_utf16_lookup_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_utf16_lookup_values.md)