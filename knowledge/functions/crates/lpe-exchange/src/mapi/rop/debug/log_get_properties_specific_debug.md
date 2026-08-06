---
type: Rust Function
title: log_get_properties_specific_debug
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L390-L526
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/modeled_zero_or_default_property
  - functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/mapi_object_debug_fields
  - functions/crates/lpe-exchange/src/mapi/rop/debug/folders/default_folder_property_mappings_for_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/is_outlook_logon_bootstrap_getprops
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_outlook_logon_bootstrap_property_details
  - functions/crates/lpe-exchange/src/mapi/rop/debug/outlook_logon_bootstrap_row_shape
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_message_body_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_default_view_entry_id_decoding
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/folders/log_calendar_default_folder_lookup_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
---

# Signature

`pub(in crate::mapi) fn log_get_properties_specific_debug( request: &RopRequest, object: Option<&MapiObject>, principal: &AccountPrincipal, columns: &[u32], mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [property_is_unsupported_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_is_unsupported_for_object.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [modeled_zero_or_default_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/modeled_zero_or_default_property.md)
- [unsupported_specific_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/unsupported_specific_property_tags.md)
- [mapi_object_debug_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/mapi_object_debug_fields.md)
- [default_folder_property_mappings_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/default_folder_property_mappings_for_debug.md)
- [format_property_value_shapes_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug.md)
- [is_outlook_logon_bootstrap_getprops](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/is_outlook_logon_bootstrap_getprops.md)
- [format_outlook_logon_bootstrap_property_details](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_outlook_logon_bootstrap_property_details.md)
- [outlook_logon_bootstrap_row_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/outlook_logon_bootstrap_row_shape.md)
- [format_ipm_configuration_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract.md)
- [format_folder_type_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract.md)
- [format_message_body_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_message_body_getprops_contract.md)
- [format_default_view_entry_id_decoding](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_default_view_entry_id_decoding.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)
- [log_common_view_descriptor_getprops_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [log_calendar_default_folder_lookup_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/folders/log_calendar_default_folder_lookup_debug.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)