---
type: Rust Function
title: format_hierarchy_debug_string
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response.rs#L174-L180
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_text_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_shape
---

# Signature

`fn format_hierarchy_debug_string(value: Option<&MapiValue>) -> String`

# Calls

- [format_debug_text_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_text_value.md)
- [mapi_value_debug_shape](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_shape.md)