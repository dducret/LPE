---
type: Rust Function
title: format_inbox_folder_type_getprops_response_context
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/values.rs#L99-L120
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_folder_type_getprops_response_context_includes_wire_preview
---

# Signature

`pub(in crate::mapi::dispatch) fn format_inbox_folder_type_getprops_response_context( response: &[u8], ) -> String`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [hex_preview](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)

# Called by

- [inbox_folder_type_getprops_response_context_includes_wire_preview](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_folder_type_getprops_response_context_includes_wire_preview.md)