---
type: Rust Function
title: default_view_table_unsupported_columns_from_summary
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L566-L579
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/calendar_event_table_column_support_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_table_column_support_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/support_field
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract
---

# Signature

`fn default_view_table_unsupported_columns_from_summary(folder_id: u64, columns: &[u32]) -> String`

# Calls

- [calendar_event_table_column_support_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/calendar_event_table_column_support_summary.md)
- [normal_message_table_column_support_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_table_column_support_summary.md)
- [support_field](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/support_field.md)

# Called by

- [format_default_view_table_compatibility_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract.md)