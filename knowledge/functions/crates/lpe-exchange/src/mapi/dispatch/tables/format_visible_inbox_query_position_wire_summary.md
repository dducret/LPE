---
type: Rust Function
title: format_visible_inbox_query_position_wire_summary
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L45-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_query_position_wire_summary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/visible_inbox_query_position_wire_summary_reports_compact_response_shape
---

# Signature

`pub(super) fn format_visible_inbox_query_position_wire_summary( request_id: &str, request_rop_names: &str, context: &str, response: &[u8], query_rows_observed: bool, ) -> String`

# Calls

- [format_query_position_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_query_position_wire_summary.md)

# Called by

- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [visible_inbox_query_position_wire_summary_reports_compact_response_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/visible_inbox_query_position_wire_summary_reports_compact_response_shape.md)