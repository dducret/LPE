---
type: Rust Function
title: default_folder_entry_id_property_name
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L207-L227
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/set_property_debug_name
---

# Signature

`pub(in crate::mapi::dispatch) fn default_folder_entry_id_property_name(tag: u32) -> &'static str`

# Calls

- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [default_folder_entry_id_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug.md)
- [set_property_debug_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/set_property_debug_name.md)