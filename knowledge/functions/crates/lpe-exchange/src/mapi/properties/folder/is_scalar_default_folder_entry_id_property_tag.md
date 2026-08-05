---
type: Rust Function
title: is_scalar_default_folder_entry_id_property_tag
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L146-L165
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_expected_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/properties/apply_mapi_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag
---

# Signature

`pub(in crate::mapi) fn is_scalar_default_folder_entry_id_property_tag(property_tag: u32) -> bool`

# Called by

- [default_folder_entry_id_expected_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_expected_folder_id.md)
- [folder_set_property_problems](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems.md)
- [default_folder_identification_safe_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value.md)
- [default_folder_entry_id_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases.md)
- [strips_default_folder_identification_value_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value_for_folder_id.md)
- [apply_mapi_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_mapi_property_values.md)
- [is_default_folder_identification_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag.md)