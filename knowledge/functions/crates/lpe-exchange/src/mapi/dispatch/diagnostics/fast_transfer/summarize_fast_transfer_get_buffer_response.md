---
type: Rust Function
title: summarize_fast_transfer_get_buffer_response
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/fast_transfer.rs#L24-L86
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/get_buffer_response_debug_exposes_wire_framing
---

# Signature

`pub(in crate::mapi::dispatch) fn summarize_fast_transfer_get_buffer_response( response: &[u8], completed: bool, ) -> FastTransferGetBufferResponseDebug`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [hex_preview](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)

# Called by

- [append_fast_transfer_source_get_buffer_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [get_buffer_response_debug_exposes_wire_framing](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/get_buffer_response_debug_exposes_wire_framing.md)