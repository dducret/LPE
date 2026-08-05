---
type: Rust Function
title: extended_folder_flags_for_search_folder
resource: crates/lpe-exchange/src/mapi/properties.rs#L669-L679
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value
---

# Signature

`fn extended_folder_flags_for_search_folder( definition: &SearchFolderDefinition, folder_id: u64, ) -> Vec<u8>`

# Calls

- [extended_folder_flags_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_folder.md)
- [search_folder_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_tag.md)

# Called by

- [search_folder_definition_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value.md)