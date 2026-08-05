---
type: Rust Function
title: format_query_position_wire_summary
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L102-L139
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_visible_inbox_query_position_wire_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_query_position_wire_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_default_view_query_position_wire_summary
---

# Signature

`fn format_query_position_wire_summary( request_id: &str, request_rop_names: &str, context: &str, response: &[u8], query_rows_observed: bool, observed_next_step: &str, missing_next_step: &str, ) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [format_visible_inbox_query_position_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_visible_inbox_query_position_wire_summary.md)
- [format_calendar_query_position_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_query_position_wire_summary.md)
- [format_default_view_query_position_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_default_view_query_position_wire_summary.md)