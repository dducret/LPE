---
type: Rust Function
title: get_properties_view_response_values_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses.rs#L760-L786
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/is_outlook_view_property_tag_for_debug
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_value_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_view_response_debug
---

# Signature

`pub(in crate::mapi::dispatch) fn get_properties_view_response_values_for_debug( property_tags: &[u32], response: &[u8], ) -> String`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [parse_property_value_for_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)
- [is_outlook_view_property_tag_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/is_outlook_view_property_tag_for_debug.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get_properties_specific_value_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/get_properties_specific_value_for_debug.md)

# Called by

- [log_get_properties_view_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_view_response_debug.md)