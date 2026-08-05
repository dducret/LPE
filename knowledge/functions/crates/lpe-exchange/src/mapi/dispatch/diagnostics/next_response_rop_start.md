---
type: Rust Function
title: next_response_rop_start
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L738-L760
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_frame_end
---

# Signature

`fn next_response_rop_start( responses: &[u8], start: usize, next_expected_rop_id: Option<u8>, ) -> Option<usize>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [read_response_error_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)

# Called by

- [response_rop_frame_end](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_frame_end.md)