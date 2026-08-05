---
type: Rust Function
title: mapi_value_debug_u32
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/values.rs#L14-L24
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_shape
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
---

# Signature

`pub(in crate::mapi::dispatch) fn mapi_value_debug_u32( properties: &HashMap<u32, MapiValue>, tag: u32, ) -> String`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mapi_value_debug_shape](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_shape.md)

# Called by

- [append_open_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)