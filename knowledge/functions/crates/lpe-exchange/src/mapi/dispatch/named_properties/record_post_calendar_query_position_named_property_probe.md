---
type: Rust Function
title: record_post_calendar_query_position_named_property_probe
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L464-L579
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_debug_named_property_sample
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/visible_inbox_release_without_query_rows_observed
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_debug_handle_table
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_live_handle_debug_summary
---

# Signature

`fn record_post_calendar_query_position_named_property_probe( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, requested_count: usize, missing_count: usize, allocated_or_store_resolved_count: usize, unresolved_count: usize, legacy_low_dynamic_property_id_count: usize, returned_count: usize, duplicate_requested_count: usize, duplicate_returned_id_count: usize, returned_id_collision_count: usize, returned_id_collisions: &str, missing_properties: &[MapiNamedProperty], property_id_source_summary: &str, property_id_mapping_summary: &str, response_rop_payload_bytes: usize, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [format_debug_named_property_sample](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_debug_named_property_sample.md)
- [visible_inbox_release_without_query_rows_observed](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/visible_inbox_release_without_query_rows_observed.md)
- [format_debug_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_debug_handle_table.md)
- [format_live_handle_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_live_handle_debug_summary.md)