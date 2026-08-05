---
type: Rust Function
title: response_handle_index_matches
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L728-L736
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_validated
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_from
---

# Signature

`fn response_handle_index_matches( responses: &[u8], start: usize, expected_response_handle_index: Option<u8>, ) -> bool`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [next_response_rop_start_validated](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_validated.md)
- [next_response_rop_start_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_from.md)