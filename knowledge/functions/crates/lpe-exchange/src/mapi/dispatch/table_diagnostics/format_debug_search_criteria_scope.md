---
type: Rust Function
title: format_debug_search_criteria_scope
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L262-L324
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_folder_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/search_criteria_debug_scope_reports_invalid_folder_ids
---

# Signature

`pub(super) fn format_debug_search_criteria_scope(request: &RopRequest) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [search_criteria_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/search_criteria_folder_ids.md)

# Called by

- [search_criteria_debug_scope_reports_invalid_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/search_criteria_debug_scope_reports_invalid_folder_ids.md)