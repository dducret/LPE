---
type: Rust Function
title: outlook_folder_view_definition
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L292-L311
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_inbox_compact_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_calendar_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_contact_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_journal_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_note_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_task_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_mail_view_definition
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/calendar_descriptor_diagnostic_detects_conflicting_descending_column_flag
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tests/folder_default_view_definitions_use_type_specific_columns
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_sort_orders
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
---

# Signature

`pub(in crate::mapi) fn outlook_folder_view_definition( folder_id: u64, view_name: &str, ) -> ViewDefinition`

# Calls

- [outlook_inbox_compact_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_inbox_compact_view_definition.md)
- [outlook_calendar_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_calendar_view_definition.md)
- [outlook_contact_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_contact_view_definition.md)
- [outlook_journal_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_journal_view_definition.md)
- [outlook_note_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_note_view_definition.md)
- [outlook_task_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_task_view_definition.md)
- [outlook_mail_view_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_mail_view_definition.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)
- [log_outlook_view_handoff](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_outlook_view_descriptor_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context.md)
- [outlook_view_descriptor_visible_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags.md)
- [format_inbox_view_descriptor_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [format_inbox_view_descriptor_set_columns_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract.md)
- [format_default_view_table_compatibility_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract.md)
- [calendar_descriptor_diagnostic_detects_conflicting_descending_column_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/calendar_descriptor_diagnostic_detects_conflicting_descending_column_flag.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [folder_default_view_definitions_use_type_specific_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/folder_default_view_definitions_use_type_specific_columns.md)
- [outlook_folder_view_sort_orders](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_sort_orders.md)
- [log_common_view_descriptor_getprops_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)