---
type: Rust Function
title: rpc_header_ext_payload
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L207-L226
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_spec
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_size_word
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer
---

# Signature

`pub(in crate::mapi) fn rpc_header_ext_payload(buffer: &[u8]) -> Option<&[u8]>`

# Calls

- [split_rop_payload_spec](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_spec.md)

# Called by

- [rop_buffer_size_word](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_size_word.md)
- [split_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_buffer.md)
- [is_rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer.md)