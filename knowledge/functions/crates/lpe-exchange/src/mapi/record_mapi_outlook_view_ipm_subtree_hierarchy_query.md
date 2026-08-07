---
type: Rust Function
title: record_mapi_outlook_view_ipm_subtree_hierarchy_query
resource: crates/lpe-exchange/src/mapi.rs#L211-L238
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response
---

# Signature

`pub(crate) fn record_mapi_outlook_view_ipm_subtree_hierarchy_query( response_row_count: u64, table_total_row_count: u64, has_conversation_action: bool, has_quick_step: bool, )`

# Called by

- [log_outlook_hierarchy_table_query_rows_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response.md)