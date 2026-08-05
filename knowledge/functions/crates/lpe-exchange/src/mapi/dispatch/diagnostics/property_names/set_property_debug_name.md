---
type: Rust Function
title: set_property_debug_name
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names.rs#L10-L252
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_property_name
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/tags/is_acl_member_name_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/format_set_property_names_for_debug
---

# Signature

`pub(in crate::mapi::dispatch) fn set_property_debug_name(tag: u32) -> &'static str`

# Calls

- [is_default_folder_identification_property_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag.md)
- [default_folder_entry_id_property_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_property_name.md)
- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [is_acl_member_name_property_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/is_acl_member_name_property_tag.md)
- [property_ids_match](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match.md)

# Called by

- [format_set_property_names_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/format_set_property_names_for_debug.md)