---
type: Rust Function
title: mapi_value_debug_shape
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/values.rs#L62-L89
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/mapi_getprops_contract_value_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_string
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_u32
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_bool
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_binary_decode
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_u32_from_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_debug_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_debug_string
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_debug_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_debug_bool
---

# Signature

`pub(in crate::mapi::dispatch) fn mapi_value_debug_shape(value: &MapiValue) -> String`

# Called by

- [mapi_getprops_contract_value_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/mapi_getprops_contract_value_debug.md)
- [mapi_value_debug_string](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_string.md)
- [mapi_value_debug_u32](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_u32.md)
- [mapi_value_debug_bool](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_bool.md)
- [mapi_value_debug_binary_decode](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_binary_decode.md)
- [mapi_value_debug_u32_from_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_u32_from_value.md)
- [format_hierarchy_debug_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_debug_folder_id.md)
- [format_hierarchy_debug_string](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_debug_string.md)
- [format_hierarchy_debug_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_debug_count.md)
- [format_hierarchy_debug_bool](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_debug_bool.md)