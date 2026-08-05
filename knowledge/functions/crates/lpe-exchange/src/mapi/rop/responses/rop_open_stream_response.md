---
type: Rust Function
title: rop_open_stream_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L180-L188
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response
---

# Signature

`pub(in crate::mapi) fn rop_open_stream_response( request: &RopRequest, stream_size: usize, ) -> Vec<u8>`

# Called by

- [append_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response.md)