---
type: Rust Function
title: normal_message_table_column_support_summary
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L215-L217
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/table_column_support_summary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_unsupported_columns_from_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/contents_table_column_support_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_covers_visible_inbox_probe_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_covers_outlook_mail_view_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_covers_observed_inbox_compact_projection
---

# Signature

`pub(super) fn normal_message_table_column_support_summary(columns: &[u32]) -> String`

# Calls

- [table_column_support_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/table_column_support_summary.md)

# Called by

- [default_view_table_unsupported_columns_from_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_table_unsupported_columns_from_summary.md)
- [contents_table_column_support_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/contents_table_column_support_summary.md)
- [normal_message_column_support_covers_visible_inbox_probe_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_covers_visible_inbox_probe_columns.md)
- [normal_message_column_support_covers_outlook_mail_view_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_covers_outlook_mail_view_columns.md)
- [normal_message_column_support_covers_observed_inbox_compact_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_message_column_support_covers_observed_inbox_compact_projection.md)