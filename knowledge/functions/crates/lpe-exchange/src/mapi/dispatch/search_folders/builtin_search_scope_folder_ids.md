---
type: Rust Function
title: builtin_search_scope_folder_ids
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L828-L836
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_criteria_to_rop
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_criteria_to_rop_for_folder_id
---

# Signature

`fn builtin_search_scope_folder_ids(role: &str) -> Option<Vec<u64>>`

# Called by

- [builtin_search_criteria_to_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_criteria_to_rop.md)
- [builtin_search_criteria_to_rop_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_criteria_to_rop_for_folder_id.md)