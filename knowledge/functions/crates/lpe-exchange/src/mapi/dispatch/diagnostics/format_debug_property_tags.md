---
type: Rust Function
title: format_debug_property_tags
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1062-L1067
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/format_calendar_required_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_property_tags
---

# Signature

`pub(super) fn format_debug_property_tags(tags: &[u32]) -> String`

# Called by

- [format_calendar_required_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/format_calendar_required_property_tags.md)
- [log_mapi_query_position_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug.md)
- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [format_debug_restriction_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction_property_tags.md)