---
type: Rust Function
title: mapi_value_debug_u32_from_value
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/values.rs#L91-L97
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_shape
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/mapi_getprops_contract_value_debug
---

# Signature

`pub(in crate::mapi::dispatch) fn mapi_value_debug_u32_from_value(value: &MapiValue) -> String`

# Calls

- [mapi_value_debug_shape](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_shape.md)

# Called by

- [mapi_getprops_contract_value_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/mapi_getprops_contract_value_debug.md)