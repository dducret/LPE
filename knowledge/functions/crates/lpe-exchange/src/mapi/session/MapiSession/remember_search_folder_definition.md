---
type: Rust Method
title: remember_search_folder_definition
resource: crates/lpe-exchange/src/mapi/session.rs#L975-L982
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_saved_search_folder_definition
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_deleted_saved_search_folder_definition
---

# Signature

`pub(in crate::mapi) fn remember_search_folder_definition( &mut self, folder_id: u64, definition: SearchFolderDefinition, )`

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [append_set_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response.md)
- [session_remembers_saved_search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_saved_search_folder_definition.md)
- [session_remembers_deleted_saved_search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_deleted_saved_search_folder_definition.md)