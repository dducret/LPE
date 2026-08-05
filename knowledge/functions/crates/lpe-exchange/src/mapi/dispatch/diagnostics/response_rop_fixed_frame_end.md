---
type: Rust Function
title: response_rop_fixed_frame_end
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L590-L633
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_frame_end
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_validated
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_from
---

# Signature

`fn response_rop_fixed_frame_end( responses: &[u8], start: usize, rop_id: u8, error_code: Option<u32>, ) -> Option<usize>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [response_rop_frame_end](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_frame_end.md)
- [next_response_rop_start_validated](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_validated.md)
- [next_response_rop_start_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_from.md)