---
type: Rust Function
title: associated_contents_table_column_support_summary
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L253-L255
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/table_column_support_summary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/contents_table_column_support_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_column_support_covers_inbox_view_descriptor_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_column_support_covers_inbox_configuration_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_column_support_covers_common_views_wlink_binary_variants
---

# Signature

`pub(super) fn associated_contents_table_column_support_summary(columns: &[u32]) -> String`

# Calls

- [table_column_support_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/table_column_support_summary.md)

# Called by

- [contents_table_column_support_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/contents_table_column_support_summary.md)
- [associated_column_support_covers_inbox_view_descriptor_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_column_support_covers_inbox_view_descriptor_columns.md)
- [associated_column_support_covers_inbox_configuration_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_column_support_covers_inbox_configuration_columns.md)
- [associated_column_support_covers_common_views_wlink_binary_variants](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_column_support_covers_common_views_wlink_binary_variants.md)