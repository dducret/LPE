---
type: Rust Function
title: search_folder_storage_type
resource: crates/lpe-exchange/src/mapi/properties/search_folders.rs#L180-L209
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_text_search
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_numerical_search
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_blob
---

# Signature

`fn search_folder_storage_type(definition: &SearchFolderDefinition) -> u32`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [search_folder_text_search](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_text_search.md)
- [search_folder_numerical_search](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_numerical_search.md)

# Called by

- [search_folder_definition_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [search_folder_definition_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_blob.md)