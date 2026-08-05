---
type: Rust Method
title: query_forward_read
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1057-L1062
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/log_sync_issues_hierarchy_query_rows
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn query_forward_read(&self) -> bool`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [log_outlook_contents_table_query_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows.md)
- [log_outlook_contents_table_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response.md)
- [log_outlook_hierarchy_table_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response.md)
- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [format_common_views_inbox_shortcut_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context.md)
- [typed](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed.md)
- [simulate_table_access](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)
- [log_sync_issues_hierarchy_query_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/log_sync_issues_hierarchy_query_rows.md)
- [rop_query_rows_response_inner](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)