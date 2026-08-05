---
type: Rust Function
title: split_rop_payload_spec
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L181-L190
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/chained_fast_transfer_get_buffer_repeats_handles_until_done
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_best_effort
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_payload
---

# Signature

`pub(in crate::mapi) fn split_rop_payload_spec(buffer: &[u8]) -> Option<(&[u8], &[u8])>`

# Called by

- [chained_fast_transfer_get_buffer_repeats_handles_until_done](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/chained_fast_transfer_get_buffer_repeats_handles_until_done.md)
- [split_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_buffer.md)
- [split_rop_payload_best_effort](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_best_effort.md)
- [rpc_header_ext_payload](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_payload.md)