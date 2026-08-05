---
type: Rust Function
title: log_calendar_default_folder_lookup_debug
resource: crates/lpe-exchange/src/mapi/rop/debug/folders.rs#L17-L125
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/mapi_object_debug_fields
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
---

# Signature

`pub(in crate::mapi) fn log_calendar_default_folder_lookup_debug( object: Option<&MapiObject>, principal: &AccountPrincipal, columns: &[u32], mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, unsupported_tags: &[u32], )`

# Calls

- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [mapi_object_debug_fields](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/mapi_object_debug_fields.md)
- [special_folder_identification_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)
- [collaboration_folder_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [format_property_value_shapes_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug.md)

# Called by

- [log_get_properties_specific_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)