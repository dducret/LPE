---
type: Rust Function
title: rpc_proxy_push_ndr_utf16_string
resource: crates/lpe-exchange/src/service/rpc_proxy_codec.rs#L29-L40
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_row
---

# Signature

`pub(super) fn rpc_proxy_push_ndr_utf16_string(buffer: &mut Vec<u8>, value: &str)`

# Calls

- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rpc_proxy_push_property_row](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_property_row.md)