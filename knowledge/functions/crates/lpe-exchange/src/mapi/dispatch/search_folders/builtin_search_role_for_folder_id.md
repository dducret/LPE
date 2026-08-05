---
type: Rust Function
title: builtin_search_role_for_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L838-L846
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_criteria_to_rop_for_folder_id
---

# Signature

`pub(super) fn builtin_search_role_for_folder_id(folder_id: u64) -> Option<&'static str>`

# Called by

- [append_set_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response.md)
- [builtin_search_criteria_to_rop_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_criteria_to_rop_for_folder_id.md)