---
type: Rust Function
title: additional_ren_entry_ids_profile_bytes
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L270-L280
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/persist_profile_folder_property_values
---

# Signature

`pub(super) fn additional_ren_entry_ids_profile_bytes(value: &MapiValue) -> Option<Vec<u8>>`

# Calls

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)

# Called by

- [folder_set_property_problems](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems.md)
- [persist_profile_folder_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/persist_profile_folder_property_values.md)