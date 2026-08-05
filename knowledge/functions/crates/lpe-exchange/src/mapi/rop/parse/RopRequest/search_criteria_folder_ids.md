---
type: Rust Method
title: search_criteria_folder_ids
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L600-L618
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_search_criteria_scope
---

# Signature

`pub(in crate::mapi) fn search_criteria_folder_ids(&self) -> Option<Vec<u64>>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [bounded_search_criteria_from_rop](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)
- [format_debug_search_criteria_scope](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_search_criteria_scope.md)