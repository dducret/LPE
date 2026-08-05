---
type: Rust Function
title: default_folder_getprops_response_values_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L229-L260
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_value_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug
---

# Signature

`pub(in crate::mapi::dispatch) fn default_folder_getprops_response_values_for_debug( property_tags: &[u32], response: &[u8], ) -> String`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [parse_property_value_for_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)
- [is_default_folder_identification_property_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_default_folder_identification_property_tag.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [default_folder_getprops_value_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_getprops_value_for_debug.md)

# Called by

- [log_get_properties_default_folder_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug.md)