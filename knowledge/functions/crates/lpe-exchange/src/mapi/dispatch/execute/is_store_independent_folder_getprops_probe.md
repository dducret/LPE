---
type: Rust Function
title: is_store_independent_folder_getprops_probe
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L134-L139
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_special_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe
---

# Signature

`fn is_store_independent_folder_getprops_probe(folder_id: u64, property_tags: &[u32]) -> bool`

# Calls

- [is_store_independent_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_special_folder.md)
- [strips_default_folder_identification_value_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_default_folder_identification_value_for_folder_id.md)

# Called by

- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)