---
type: Rust Function
title: rpc_header_ext_rop_buffer_chain
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L246-L265
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer_with_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/chained_fast_transfer_get_buffer_repeats_handles_until_done
---

# Signature

`pub(in crate::mapi) fn rpc_header_ext_rop_buffer_chain( mut first: Vec<u8>, additional_payloads: Vec<Vec<u8>>, ) -> Vec<u8>`

# Calls

- [rpc_header_ext_rop_buffer_with_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer_with_flags.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [chained_fast_transfer_get_buffer_repeats_handles_until_done](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/chained_fast_transfer_get_buffer_repeats_handles_until_done.md)