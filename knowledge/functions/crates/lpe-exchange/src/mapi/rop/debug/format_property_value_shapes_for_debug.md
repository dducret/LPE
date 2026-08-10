---
type: Rust Function
title: format_property_value_shapes_for_debug
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L774-L817
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-exchange/src/mapi/rop/modeled_zero_or_default_property
  - functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/folders/log_calendar_default_folder_lookup_debug
---

# Signature

`pub(in crate::mapi) fn format_property_value_shapes_for_debug( object: Option<&MapiObject>, principal: &AccountPrincipal, columns: &[u32], mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, unsupported_tags: &[u32], ) -> String`

# Calls

- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [modeled_zero_or_default_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/modeled_zero_or_default_property.md)
- [semantic_property_shape_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug.md)

# Called by

- [log_get_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [log_calendar_default_folder_lookup_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/log_calendar_default_folder_lookup_debug.md)