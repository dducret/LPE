---
type: Rust Function
title: format_debug_named_property_context
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties.rs#L27-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_name_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/debug_named_property_context_reports_session_and_unresolved_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_reports_calendar_lids
---

# Signature

`pub(in crate::mapi::dispatch) fn format_debug_named_property_context( session: &MapiSession, tags: &[u32], ) -> String`

# Calls

- [property_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [property_type_code](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [well_known_named_property_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id.md)
- [property_name_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_name_for_id.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)
- [format_outlook_view_descriptor_named_property_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context.md)
- [format_contents_table_named_property_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context.md)
- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [append_set_columns_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [debug_named_property_context_reports_session_and_unresolved_properties](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/debug_named_property_context_reports_session_and_unresolved_properties.md)
- [outlook_view_descriptor_named_property_context_reports_calendar_lids](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_reports_calendar_lids.md)