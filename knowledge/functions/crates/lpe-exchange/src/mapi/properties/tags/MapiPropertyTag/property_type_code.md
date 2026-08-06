---
type: Rust Method
title: property_type_code
resource: crates/lpe-exchange/src/mapi/properties/tags.rs#L17-L19
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/staged_contact_commit_input
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_event_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_contact_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/staged_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tag_for_session
  - functions/crates/lpe-exchange/src/mapi/properties/contact/outlook_contact_source_empty_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag
---

# Signature

`pub(in crate::mapi) fn property_type_code(self) -> u16`

# Called by

- [staged_contact_commit_input](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/staged_contact_commit_input.md)
- [upsert_custom_property_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values.md)
- [mapi_event_custom_property_values_from_map](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_event_custom_property_values_from_map.md)
- [mapi_contact_custom_property_values_from_map](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_contact_custom_property_values_from_map.md)
- [staged_custom_property_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/staged_custom_property_values.md)
- [format_debug_named_property_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context.md)
- [staged_event_commit_input](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input.md)
- [normalize_table_property_tag_for_session](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tag_for_session.md)
- [outlook_contact_source_empty_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/outlook_contact_source_empty_value.md)
- [property_type](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)
- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [get_properties_specific_typed_value_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag.md)
- [normalize_named_property_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag.md)