---
type: Rust Function
title: user_saved_search_folder_is_projectable
resource: crates/lpe-exchange/src/mapi_store.rs#L385-L403
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/user_saved_search_folder_definition_by_display_name
---

# Signature

`fn user_saved_search_folder_is_projectable(definition: &SearchFolderDefinition) -> bool`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [with_search_folder_definitions](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions.md)
- [user_saved_search_folder_definition_by_display_name](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/user_saved_search_folder_definition_by_display_name.md)