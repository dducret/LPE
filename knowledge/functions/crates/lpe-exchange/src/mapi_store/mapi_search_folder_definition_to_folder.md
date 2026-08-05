---
type: Rust Function
title: mapi_search_folder_definition_to_folder
resource: crates/lpe-exchange/src/mapi_store.rs#L351-L374
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi_store/mapi_search_folder_role
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions
---

# Signature

`fn mapi_search_folder_definition_to_folder( definition: &SearchFolderDefinition, ) -> Option<MapiFolder>`

# Calls

- [mapped_mapi_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [mapi_search_folder_role](../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_search_folder_role.md)
- [global_counter_from_store_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [with_search_folder_definitions](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions.md)