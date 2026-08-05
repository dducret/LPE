---
type: Rust Function
title: rop_buffer_size_word
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L770-L774
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_payload
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_layout_name
---

# Signature

`fn rop_buffer_size_word(rop_buffer: &[u8]) -> Option<u16>`

# Calls

- [rpc_header_ext_payload](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_payload.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)
- [rop_buffer_layout_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_layout_name.md)