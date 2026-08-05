---
type: Rust Function
title: strips_default_folder_identification_value
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L381-L388
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_values_stripped_by_safe_values
---

# Signature

`fn strips_default_folder_identification_value(object: Option<&MapiObject>, tag: u32) -> bool`

# Calls

- [strips_default_folder_identification_value_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value_for_folder_id.md)

# Called by

- [default_folder_identification_safe_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value.md)
- [default_folder_identification_values_stripped_by_safe_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_values_stripped_by_safe_values.md)