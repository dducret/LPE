---
type: Rust Function
title: rpc_proxy_nspi_requested_mids
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L1105-L1117
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_requested_nspi_entry
---

# Signature

`fn rpc_proxy_nspi_requested_mids(request: &[u8]) -> Vec<u32>`

# Calls

- [read_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rpc_proxy_requested_nspi_entry](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_requested_nspi_entry.md)