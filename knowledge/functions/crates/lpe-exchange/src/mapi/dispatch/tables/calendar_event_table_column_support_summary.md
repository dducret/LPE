---
type: Rust Function
title: calendar_event_table_column_support_summary
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L257-L259
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/table_column_support_summary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_unsupported_columns_from_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_event_column_support_covers_observed_outlook_view_probe_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_event_column_support_reports_unknown_named_properties_as_dynamic
---

# Signature

`pub(super) fn calendar_event_table_column_support_summary(columns: &[u32]) -> String`

# Calls

- [table_column_support_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/table_column_support_summary.md)

# Called by

- [default_view_table_unsupported_columns_from_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_unsupported_columns_from_summary.md)
- [calendar_event_column_support_covers_observed_outlook_view_probe_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_event_column_support_covers_observed_outlook_view_probe_columns.md)
- [calendar_event_column_support_reports_unknown_named_properties_as_dynamic](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_event_column_support_reports_unknown_named_properties_as_dynamic.md)