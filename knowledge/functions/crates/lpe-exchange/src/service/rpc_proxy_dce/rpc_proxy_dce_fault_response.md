---
type: Rust Function
title: rpc_proxy_dce_fault_response
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L133-L160
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_for_principal
---

# Signature

`pub(super) fn rpc_proxy_dce_fault_response(call_id: u32, status: u32) -> Vec<u8>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rpc_proxy_emsmdb_rpc_ext2_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_for_principal.md)