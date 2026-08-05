---
type: Rust Function
title: rpc_proxy_nspi_entry_exact_match
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1080-L1090
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_match_nspi_entry
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_matches
---

# Signature

`fn rpc_proxy_nspi_entry_exact_match(entry: &ExchangeAddressBookEntry, value: &str) -> bool`

# Calls

- [rpc_proxy_normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value.md)

# Called by

- [rpc_proxy_match_nspi_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_match_nspi_entry.md)
- [rpc_proxy_nspi_entry_matches](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_matches.md)