---
type: Rust Function
title: previous_mapi_bounded_scope_json
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L401-L416
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop
---

# Signature

`fn previous_mapi_bounded_scope_json(definition: Option<&SearchFolderDefinition>) -> Option<Value>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [bounded_search_criteria_from_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)