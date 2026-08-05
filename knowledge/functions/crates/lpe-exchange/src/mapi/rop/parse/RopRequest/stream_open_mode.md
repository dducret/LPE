---
type: Rust Method
title: stream_open_mode
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L191-L193
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
---

# Signature

`pub(in crate::mapi) fn stream_open_mode(&self) -> Option<u8>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_open_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)