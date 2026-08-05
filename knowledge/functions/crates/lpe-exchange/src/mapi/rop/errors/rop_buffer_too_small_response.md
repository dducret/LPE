---
type: Rust Function
title: rop_buffer_too_small_response
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L117-L133
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/apply_execute_max_rop_out
  - functions/crates/lpe-exchange/src/mapi/rop/tests/buffer_too_small_response_matches_microsoft_rop_layout
---

# Signature

`pub(in crate::mapi) fn rop_buffer_too_small_response( size_needed: usize, request_buffers: &[u8], handle_table: &[u8], ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [apply_execute_max_rop_out](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/apply_execute_max_rop_out.md)
- [buffer_too_small_response_matches_microsoft_rop_layout](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/buffer_too_small_response_matches_microsoft_rop_layout.md)