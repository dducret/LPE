---
type: Rust Function
title: strips_any_default_folder_identification_values
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L406-L414
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases
---

# Signature

`fn strips_any_default_folder_identification_values(object: Option<&MapiObject>) -> bool`

# Called by

- [default_folder_identification_safe_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_values.md)
- [default_folder_entry_id_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases.md)