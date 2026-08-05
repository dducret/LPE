---
type: Rust Function
title: mapi_value_debug_binary_decode
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/values.rs#L37-L54
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

`pub(in crate::mapi::dispatch) fn mapi_value_debug_binary_decode( properties: &HashMap<u32, MapiValue>, tag: u32, ) -> String`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mapi_value_debug_shape](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_shape.md)

# Called by

- [append_open_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)