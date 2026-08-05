---
type: Rust Function
title: search_folder_definition_blob
resource: crates/lpe-exchange/src/mapi/properties/search_folders.rs#L211-L238
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_storage_type
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_numerical_search
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/write_search_folder_text_search
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_text_search
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
---

# Signature

`fn search_folder_definition_blob(definition: &SearchFolderDefinition) -> Vec<u8>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [search_folder_storage_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_storage_type.md)
- [search_folder_numerical_search](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_numerical_search.md)
- [write_search_folder_text_search](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/write_search_folder_text_search.md)
- [search_folder_text_search](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_text_search.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [search_folder_definition_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)