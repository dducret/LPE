---
type: Rust Method
title: user_saved_search_folder_definition_by_display_name
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1254-L1270
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/user_saved_search_folder_is_projectable
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
---

# Signature

`pub(crate) fn user_saved_search_folder_definition_by_display_name( &self, display_name: &str, result_object_kind: &str, ) -> Option<&SearchFolderDefinition>`

# Calls

- [user_saved_search_folder_is_projectable](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/user_saved_search_folder_is_projectable.md)

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)