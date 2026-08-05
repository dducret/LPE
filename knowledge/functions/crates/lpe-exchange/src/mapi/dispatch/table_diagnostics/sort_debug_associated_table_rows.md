---
type: Rust Function
title: sort_debug_associated_table_rows
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L488-L509
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/compare_debug_mapi_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_inbox_associated_query_row_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
---

# Signature

`pub(super) fn sort_debug_associated_table_rows( rows: &mut [DebugAssociatedTableRow], sort_orders: &[MapiSortOrder], mailbox_guid: Uuid, )`

# Calls

- [compare_debug_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/compare_debug_mapi_values.md)
- [debug_associated_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_property_value.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)
- [debug_associated_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_id.md)

# Called by

- [format_inbox_associated_wire_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary.md)
- [format_inbox_associated_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_inbox_associated_query_row_window.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)