---
type: Rust Function
title: default_folder_property_mapping_for_debug
resource: crates/lpe-exchange/src/mapi/rop/debug/folders.rs#L133-L156
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/folders/default_folder_property_mappings_for_debug
---

# Signature

`fn default_folder_property_mapping_for_debug(tag: u32) -> Option<String>`

# Calls

- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [default_folder_property_mappings_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/default_folder_property_mappings_for_debug.md)