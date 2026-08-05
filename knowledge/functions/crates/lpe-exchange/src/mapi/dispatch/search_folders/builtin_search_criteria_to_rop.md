---
type: Rust Function
title: builtin_search_criteria_to_rop
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L814-L826
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_scope_folder_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response
---

# Signature

`pub(super) fn builtin_search_criteria_to_rop( definition: &lpe_storage::SearchFolderDefinition, ) -> Option<(Vec<u8>, Vec<u64>, u32)>`

# Calls

- [builtin_search_scope_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/builtin_search_scope_folder_ids.md)

# Called by

- [append_get_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response.md)