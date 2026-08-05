---
type: Rust Function
title: rpc_proxy_nspi_entry_id
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L973-L983
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_row_values_for_entry
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_requested_nspi_entry
---

# Signature

`fn rpc_proxy_nspi_entry_id(entry: &ExchangeAddressBookEntry) -> u32`

# Called by

- [rpc_proxy_nspi_get_matches_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal.md)
- [rpc_proxy_nspi_row_values_for_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_row_values_for_entry.md)
- [rpc_proxy_requested_nspi_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_requested_nspi_entry.md)