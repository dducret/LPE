---
type: Rust Function
title: strips_default_folder_identification_value_for_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L390-L404
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_folder_getprops_probe
---

# Signature

`pub(super) fn strips_default_folder_identification_value_for_folder_id( folder_id: u64, tag: u32, ) -> bool`

# Calls

- [is_default_folder_identification_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag.md)
- [is_scalar_default_folder_entry_id_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag.md)

# Called by

- [strips_default_folder_identification_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value.md)
- [is_store_independent_folder_getprops_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_folder_getprops_probe.md)