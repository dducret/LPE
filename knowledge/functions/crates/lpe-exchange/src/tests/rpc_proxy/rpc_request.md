---
type: Rust Function
title: rpc_request
resource: crates/lpe-exchange/src/tests/rpc_proxy.rs#L1645-L1655
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/nspi_rpc_request
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/rfri_rpc_request
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_request
  - functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_ext2_request
---

# Signature

`fn rpc_request(call_id: u32, context_id: u16, opnum: u16, fragment_length: usize) -> Vec<u8>`

# Called by

- [nspi_rpc_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/nspi_rpc_request.md)
- [rfri_rpc_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/rfri_rpc_request.md)
- [emsmdb_rpc_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_request.md)
- [emsmdb_rpc_ext2_request](../../../../../../functions/crates/lpe-exchange/src/tests/rpc_proxy/emsmdb_rpc_ext2_request.md)