---
type: Rust Function
title: rop_buffer_layout_name
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L776-L796
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_size_word
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
---

# Signature

`fn rop_buffer_layout_name(rop_buffer: &[u8]) -> &'static str`

# Calls

- [rop_buffer_size_word](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_size_word.md)
- [is_rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer.md)

# Called by

- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)