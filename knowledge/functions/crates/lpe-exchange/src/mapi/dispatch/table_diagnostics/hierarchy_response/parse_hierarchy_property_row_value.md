---
type: Rust Function
title: parse_hierarchy_property_row_value
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response.rs#L9-L30
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/hierarchy_response_metric_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_query_rows_wire_summary
---

# Signature

`fn parse_hierarchy_property_row_value( cursor: &mut Cursor<'_>, row_status: u8, property_tag: u32, ) -> Result<Option<MapiValue>>`

# Calls

- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [read_u8](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)

# Called by

- [hierarchy_response_metric_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/hierarchy_response_metric_summary.md)
- [format_hierarchy_query_rows_wire_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_query_rows_wire_summary.md)