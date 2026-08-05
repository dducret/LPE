---
type: Rust Function
title: log_common_view_descriptor_getprops_summary
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L973-L1052
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/common_view_descriptor_property_requested
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings
  - functions/crates/lpe-exchange/src/mapi/rop/utf16le_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/debug/view_descriptor_debug_property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_requested_view_descriptor_contract
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_response_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
---

# Signature

`pub(in crate::mapi) fn log_common_view_descriptor_getprops_summary( principal: &AccountPrincipal, request: &RopRequest, object: Option<&MapiObject>, columns: &[u32], snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [common_view_descriptor_property_requested](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/common_view_descriptor_property_requested.md)
- [common_view_named_view_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id.md)
- [default_folder_named_view_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message.md)
- [outlook_folder_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [view_descriptor_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_strings](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings.md)
- [utf16le_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/utf16le_bytes.md)
- [view_descriptor_debug_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/view_descriptor_debug_property_tags.md)
- [format_requested_view_descriptor_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_requested_view_descriptor_contract.md)
- [format_common_view_descriptor_response_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_response_values.md)

# Called by

- [log_get_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)