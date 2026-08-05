---
type: Rust Function
title: rpc_proxy_push_property_row
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1189-L1212
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_utf16_string
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_rowset_pointer
---

# Signature

`fn rpc_proxy_push_property_row(buffer: &mut Vec<u8>, row_values: &[(u32, RpcProxyNspiValue)])`

# Calls

- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)
- [rpc_proxy_push_ndr_utf16_string](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_utf16_string.md)
- [rpc_proxy_push_ndr_ascii_string](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_ascii_string.md)

# Called by

- [rpc_proxy_nspi_get_props_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response.md)
- [rpc_proxy_nspi_get_props_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_nspi_get_props_response_for_principal.md)
- [rpc_proxy_push_rowset_pointer](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_rowset_pointer.md)