---
type: Rust Function
title: rpc_proxy_nspi_row_values
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L892-L916
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_smtp_address
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_display_name_for_smtp_address
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response
---

# Signature

`fn rpc_proxy_nspi_row_values(request: &[u8], tags: &[u32]) -> Vec<(u32, RpcProxyNspiValue)>`

# Calls

- [rpc_proxy_nspi_requested_smtp_address](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_smtp_address.md)
- [rpc_proxy_display_name_for_smtp_address](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_display_name_for_smtp_address.md)

# Called by

- [rpc_proxy_nspi_query_rows_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_query_rows_response.md)
- [rpc_proxy_nspi_get_matches_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_matches_response.md)
- [rpc_proxy_nspi_get_props_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response.md)