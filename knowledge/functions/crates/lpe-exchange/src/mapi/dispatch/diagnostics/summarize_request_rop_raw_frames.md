---
type: Rust Function
title: summarize_request_rop_raw_frames
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L305-L356
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
---

# Signature

`pub(super) fn summarize_request_rop_raw_frames(requests: &[u8]) -> (usize, String)`

# Calls

- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [summarize_request_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)