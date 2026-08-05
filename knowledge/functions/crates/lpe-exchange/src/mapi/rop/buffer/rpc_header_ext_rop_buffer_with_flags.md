---
type: Rust Function
title: rpc_header_ext_rop_buffer_with_flags
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L232-L244
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer_chain
---

# Signature

`pub(in crate::mapi) fn rpc_header_ext_rop_buffer_with_flags( payload: Vec<u8>, flags: u16, ) -> Vec<u8>`

# Called by

- [rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer.md)
- [rpc_header_ext_rop_buffer_chain](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer_chain.md)