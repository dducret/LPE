---
type: Rust Function
title: format_common_view_descriptor_getprops_contract
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L1054-L1128
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/view_descriptor_debug_property_tags
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings
  - functions/crates/lpe-exchange/src/mapi/rop/utf16le_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_response_values
  - functions/crates/lpe-exchange/src/mapi/rop/debug/default_view_message_entry_id_target
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/tests/common_view_descriptor_getprops_contract_reports_unpersisted_view_missing
---

# Signature

`pub(in crate::mapi) fn format_common_view_descriptor_getprops_contract( object: Option<&MapiObject>, principal: &AccountPrincipal, columns: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [common_view_named_view_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id.md)
- [default_folder_named_view_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message.md)
- [outlook_folder_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [view_descriptor_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_debug_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/view_descriptor_debug_property_tags.md)
- [view_descriptor_strings](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings.md)
- [utf16le_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/utf16le_bytes.md)
- [format_common_view_descriptor_response_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_response_values.md)
- [default_view_message_entry_id_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/default_view_message_entry_id_target.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [log_get_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [common_view_descriptor_getprops_contract_reports_unpersisted_view_missing](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/common_view_descriptor_getprops_contract_reports_unpersisted_view_missing.md)