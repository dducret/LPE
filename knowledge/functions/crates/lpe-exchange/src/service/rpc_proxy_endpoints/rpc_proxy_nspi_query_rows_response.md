---
type: Rust Function
title: rpc_proxy_nspi_query_rows_response
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L431-L440
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_property_tags
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_row_values
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_stat
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_rowset_pointer
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum
---

# Signature

`fn rpc_proxy_nspi_query_rows_response(call_id: u32, request: &[u8]) -> Vec<u8>`

# Calls

- [rpc_proxy_nspi_requested_property_tags](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_requested_property_tags.md)
- [rpc_proxy_nspi_row_values](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_row_values.md)
- [rpc_proxy_push_stat](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_stat.md)
- [rpc_proxy_push_rowset_pointer](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_rowset_pointer.md)
- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)
- [rpc_proxy_dce_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response.md)

# Called by

- [rpc_proxy_nspi_response_for_opnum](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_response_for_opnum.md)