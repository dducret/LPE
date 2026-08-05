---
type: Rust Function
title: split_rop_buffer
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L157-L162
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_payload
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_spec
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_best_effort
---

# Signature

`pub(in crate::mapi) fn split_rop_buffer(buffer: &[u8]) -> Option<(&[u8], &[u8])>`

# Calls

- [rpc_header_ext_payload](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_payload.md)
- [split_rop_payload_spec](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_spec.md)
- [split_rop_payload_best_effort](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_best_effort.md)