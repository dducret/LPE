---
type: Rust Function
title: summarize_response_rop_frame
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1020-L1049
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
---

# Signature

`pub(super) fn summarize_response_rop_frame( responses: &[u8], start: usize, end: usize, error_code: Option<u32>, ) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)