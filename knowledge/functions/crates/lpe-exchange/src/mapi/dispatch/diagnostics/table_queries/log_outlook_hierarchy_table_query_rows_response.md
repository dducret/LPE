---
type: Rust Function
title: log_outlook_hierarchy_table_query_rows_response
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L941-L1029
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_query_rows_wire_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/hierarchy_response_metric_summary
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_ipm_subtree_hierarchy_query
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_outlook_hierarchy_table_query_rows_response( principal: &AccountPrincipal, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, response: &[u8], mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, queried_position: usize, )`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [query_forward_read](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read.md)
- [default_hierarchy_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns.md)
- [table_position_and_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [hex_preview](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)
- [format_hierarchy_query_rows_wire_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_query_rows_wire_summary.md)
- [hierarchy_response_metric_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/hierarchy_response_metric_summary.md)
- [record_mapi_outlook_view_ipm_subtree_hierarchy_query](../../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_ipm_subtree_hierarchy_query.md)

# Called by

- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)