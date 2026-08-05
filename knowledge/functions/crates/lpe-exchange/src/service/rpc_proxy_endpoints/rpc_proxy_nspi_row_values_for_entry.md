---
type: Rust Function
title: rpc_proxy_nspi_row_values_for_entry
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L945-L971
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal
---

# Signature

`fn rpc_proxy_nspi_row_values_for_entry( tags: &[u32], entry: &ExchangeAddressBookEntry, ) -> Vec<(u32, RpcProxyNspiValue)>`

# Calls

- [rpc_proxy_nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_entry_id.md)

# Called by

- [rpc_proxy_nspi_query_rows_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response_for_principal.md)
- [rpc_proxy_nspi_get_matches_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response_for_principal.md)
- [rpc_proxy_nspi_get_props_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal.md)