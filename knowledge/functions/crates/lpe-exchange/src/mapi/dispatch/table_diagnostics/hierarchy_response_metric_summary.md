---
type: Rust Function
title: hierarchy_response_metric_summary
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L1392-L1428
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_metric_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response
---

# Signature

`pub(super) fn hierarchy_response_metric_summary( response: &[u8], selected_columns: &[u32], ) -> HierarchyResponseMetricSummary`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [parse_mapi_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [hierarchy_metric_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_metric_folder_id.md)

# Called by

- [log_outlook_hierarchy_table_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response.md)