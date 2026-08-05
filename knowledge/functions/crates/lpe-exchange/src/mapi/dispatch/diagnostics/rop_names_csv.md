---
type: Rust Function
title: rop_names_csv
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L275-L281
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
---

# Signature

`pub(super) fn rop_names_csv(rop_ids: &[u8]) -> String`

# Calls

- [rop_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_name.md)

# Called by

- [summarize_request_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)