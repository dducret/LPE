---
type: Rust Function
title: format_default_view_query_position_wire_summary
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L81-L100
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_query_position_wire_summary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_query_position_wire_summary_reports_role_specific_next_step
---

# Signature

`pub(super) fn format_default_view_query_position_wire_summary( request_id: &str, request_rop_names: &str, context: &str, response: &[u8], query_rows_observed_on_handle: bool, role: &str, ) -> String`

# Calls

- [format_query_position_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_query_position_wire_summary.md)

# Called by

- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [default_view_query_position_wire_summary_reports_role_specific_next_step](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_query_position_wire_summary_reports_role_specific_next_step.md)