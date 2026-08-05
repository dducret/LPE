---
type: Rust Function
title: default_folder_entry_id_expected_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L3-L23
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug
---

# Signature

`pub(super) fn default_folder_entry_id_expected_folder_id(tag: u32) -> Option<u64>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [is_scalar_default_folder_entry_id_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag.md)

# Called by

- [folder_set_property_problems](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/folder_set_property_problems.md)
- [default_folder_entry_id_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases.md)
- [default_folder_entry_id_values_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug.md)