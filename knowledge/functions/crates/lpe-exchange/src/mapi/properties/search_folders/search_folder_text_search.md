---
type: Rust Function
title: search_folder_text_search
resource: crates/lpe-exchange/src/mapi/properties/search_folders.rs#L240-L249
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_storage_type
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_blob
---

# Signature

`fn search_folder_text_search(definition: &SearchFolderDefinition) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [search_folder_storage_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_storage_type.md)
- [search_folder_definition_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_blob.md)