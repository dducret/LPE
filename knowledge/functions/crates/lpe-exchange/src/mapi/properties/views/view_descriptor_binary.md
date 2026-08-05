---
type: Rust Function
title: view_descriptor_binary
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L610-L653
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/views/write_view_column_packet
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_handoff_descriptor_summary_reports_outlook_view_shape
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/calendar_descriptor_diagnostic_detects_conflicting_descending_column_flag
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tests/messages_view_definition_matches_outlook_visible_inbox_projection
  - functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_compact_view_definition_binary_matches_visible_trace_contract
  - functions/crates/lpe-exchange/src/mapi/properties/tests/view_descriptor_named_string_column_matches_microsoft_example
  - functions/crates/lpe-exchange/src/mapi/properties/tests/common_view_sent_to_descriptor_uses_recipient_columns
  - functions/crates/lpe-exchange/src/mapi/properties/tests/folder_default_view_definitions_use_type_specific_columns
  - functions/crates/lpe-exchange/src/mapi/properties/views/log_view_definition_diagnostics
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_view_named_view_sync_projects_canonical_descriptor_properties
---

# Signature

`pub(in crate::mapi) fn view_descriptor_binary(definition: &ViewDefinition) -> Vec<u8>`

# Calls

- [write_view_column_packet](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/write_view_column_packet.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)
- [log_outlook_view_handoff](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_outlook_view_descriptor_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context.md)
- [outlook_view_descriptor_visible_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags.md)
- [format_inbox_view_descriptor_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [format_inbox_view_descriptor_set_columns_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract.md)
- [format_default_view_table_compatibility_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract.md)
- [view_handoff_descriptor_summary_reports_outlook_view_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_handoff_descriptor_summary_reports_outlook_view_shape.md)
- [calendar_descriptor_diagnostic_detects_conflicting_descending_column_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/calendar_descriptor_diagnostic_detects_conflicting_descending_column_flag.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [messages_view_definition_matches_outlook_visible_inbox_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/messages_view_definition_matches_outlook_visible_inbox_projection.md)
- [outlook_compact_view_definition_binary_matches_visible_trace_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/outlook_compact_view_definition_binary_matches_visible_trace_contract.md)
- [view_descriptor_named_string_column_matches_microsoft_example](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/view_descriptor_named_string_column_matches_microsoft_example.md)
- [common_view_sent_to_descriptor_uses_recipient_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/common_view_sent_to_descriptor_uses_recipient_columns.md)
- [folder_default_view_definitions_use_type_specific_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/folder_default_view_definitions_use_type_specific_columns.md)
- [log_view_definition_diagnostics](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/log_view_definition_diagnostics.md)
- [log_common_view_descriptor_getprops_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)
- [common_view_named_view_sync_projects_canonical_descriptor_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_view_named_view_sync_projects_canonical_descriptor_properties.md)