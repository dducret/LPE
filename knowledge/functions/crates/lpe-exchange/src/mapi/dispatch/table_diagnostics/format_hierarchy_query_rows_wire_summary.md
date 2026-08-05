---
type: Rust Function
title: format_hierarchy_query_rows_wire_summary
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L1444-L1508
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/hierarchy_query_rows_wire_summary_decodes_compact_folder_projection
---

# Signature

`pub(super) fn format_hierarchy_query_rows_wire_summary( response: &[u8], selected_columns: &[u32], max_rows: usize, ) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_mapi_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)

# Called by

- [log_outlook_hierarchy_table_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response.md)
- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [hierarchy_query_rows_wire_summary_decodes_compact_folder_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/hierarchy_query_rows_wire_summary_decodes_compact_folder_projection.md)