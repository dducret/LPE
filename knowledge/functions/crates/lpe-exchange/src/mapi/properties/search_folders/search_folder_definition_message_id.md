---
type: Rust Function
title: search_folder_definition_message_id
resource: crates/lpe-exchange/src/mapi/properties/search_folders.rs#L161-L163
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
---

# Signature

`fn search_folder_definition_message_id(definition: &SearchFolderDefinition) -> Option<u64>`

# Calls

- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [search_folder_definition_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)