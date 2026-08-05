---
type: Rust Function
title: rpc_proxy_nspi_entry_matches
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1092-L1103
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_exact_match
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_filter_nspi_entries
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_match_nspi_entry
---

# Signature

`fn rpc_proxy_nspi_entry_matches(entry: &ExchangeAddressBookEntry, value: &str) -> bool`

# Calls

- [rpc_proxy_normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_normalize_nspi_lookup_value.md)
- [rpc_proxy_nspi_entry_exact_match](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_exact_match.md)

# Called by

- [rpc_proxy_filter_nspi_entries](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_filter_nspi_entries.md)
- [rpc_proxy_match_nspi_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_match_nspi_entry.md)