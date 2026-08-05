---
type: Rust Function
title: default_folder_property_mappings_for_debug
resource: crates/lpe-exchange/src/mapi/rop/debug/folders.rs#L127-L131
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/folders/default_folder_property_mapping_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
---

# Signature

`pub(in crate::mapi) fn default_folder_property_mappings_for_debug(tags: &[u32]) -> Vec<String>`

# Calls

- [default_folder_property_mapping_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/default_folder_property_mapping_for_debug.md)

# Called by

- [log_get_properties_specific_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)