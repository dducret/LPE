---
type: Rust Method
title: search_folder_definition
resource: crates/lpe-exchange/src/mapi/session.rs#L984-L991
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response
---

# Signature

`pub(in crate::mapi) fn search_folder_definition( &self, folder_id: u64, ) -> Option<&SearchFolderDefinition>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [append_set_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response.md)
- [append_get_search_criteria_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response.md)