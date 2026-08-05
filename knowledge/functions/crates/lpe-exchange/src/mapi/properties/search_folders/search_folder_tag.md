---
type: Rust Function
title: search_folder_tag
resource: crates/lpe-exchange/src/mapi/properties/search_folders.rs#L304-L314
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_search_folder
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
---

# Signature

`pub(in crate::mapi) fn search_folder_tag(definition: &SearchFolderDefinition) -> u32`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [extended_folder_flags_for_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_search_folder.md)
- [search_folder_definition_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)