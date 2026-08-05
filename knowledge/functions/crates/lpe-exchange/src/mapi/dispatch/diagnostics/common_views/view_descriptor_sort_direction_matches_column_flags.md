---
type: Rust Function
title: view_descriptor_sort_direction_matches_column_flags
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L718-L732
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_contract_invariant_issues
---

# Signature

`pub(in crate::mapi::dispatch) fn view_descriptor_sort_direction_matches_column_flags( descriptor: &[u8], ) -> Option<bool>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [view_descriptor_column_flags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_column_flags.md)

# Called by

- [format_calendar_contract_invariant_issues](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_contract_invariant_issues.md)