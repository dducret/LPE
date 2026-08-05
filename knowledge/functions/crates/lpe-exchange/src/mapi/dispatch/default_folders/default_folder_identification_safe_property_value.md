---
type: Rust Function
title: default_folder_identification_safe_property_value
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L168-L198
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_additional_ren_entry_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_indexed_special_folder_entry_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_values
---

# Signature

`fn default_folder_identification_safe_property_value( principal: &AccountPrincipal, object: Option<&MapiObject>, tag: u32, value: MapiValue, ) -> Option<(u32, MapiValue)>`

# Calls

- [strips_default_folder_identification_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [is_scalar_default_folder_entry_id_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag.md)
- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [merge_additional_ren_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_additional_ren_entry_ids.md)
- [merge_indexed_special_folder_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/merge_indexed_special_folder_entry_ids.md)

# Called by

- [default_folder_identification_safe_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_values.md)