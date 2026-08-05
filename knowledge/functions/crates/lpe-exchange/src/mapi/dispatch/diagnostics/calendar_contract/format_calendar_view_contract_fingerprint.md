---
type: Rust Function
title: format_calendar_view_contract_fingerprint
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract.rs#L10-L158
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/escape_contract_text
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings_binary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/view_descriptor_debug_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_fai_inventory
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_named_property_registry
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_named_property_id_reuse
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_sort_orders
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_option
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_contract_invariant_issues
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/log_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_covers_client_normal_view_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_covers_exact_selected_table_state
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_bounds_large_named_property_registry
---

# Signature

`pub(in crate::mapi::dispatch) fn format_calendar_view_contract_fingerprint( session: &MapiSession, account_id: Uuid, stage: &str, object: Option<&MapiObject>, query_position_response: Option<(u32, u32)>, snapshot: &MapiMailStoreSnapshot, ) -> Option<String>`

# Calls

- [debug_advertised_default_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)
- [outlook_folder_view_definition](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [escape_contract_text](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/escape_contract_text.md)
- [view_descriptor_binary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_strings_binary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings_binary.md)
- [view_descriptor_debug_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/view_descriptor_debug_property_tags.md)
- [format_calendar_fai_inventory](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_fai_inventory.md)
- [format_named_property_registry](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_named_property_registry.md)
- [format_named_property_id_reuse](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_named_property_id_reuse.md)
- [format_debug_named_property_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_calendar_event_query_position_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
- [format_debug_sort_orders](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_sort_orders.md)
- [format_debug_restriction_option](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_option.md)
- [format_calendar_contract_invariant_issues](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_contract_invariant_issues.md)

# Called by

- [log_calendar_view_contract_fingerprint](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/log_calendar_view_contract_fingerprint.md)
- [calendar_contract_fingerprint_covers_client_normal_view_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_covers_client_normal_view_contract.md)
- [calendar_contract_fingerprint_covers_exact_selected_table_state](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_covers_exact_selected_table_state.md)
- [calendar_contract_fingerprint_bounds_large_named_property_registry](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_bounds_large_named_property_registry.md)