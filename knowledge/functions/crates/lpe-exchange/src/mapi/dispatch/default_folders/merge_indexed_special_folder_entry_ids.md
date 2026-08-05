---
type: Rust Function
title: merge_indexed_special_folder_entry_ids
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L200-L218
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value
---

# Signature

`fn merge_indexed_special_folder_entry_ids( principal: &AccountPrincipal, tag: u32, value: MapiValue, ) -> Option<MapiValue>`

# Calls

- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)

# Called by

- [default_folder_identification_safe_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value.md)