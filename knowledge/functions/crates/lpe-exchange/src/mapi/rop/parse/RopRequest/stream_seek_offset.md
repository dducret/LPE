---
type: Rust Method
title: stream_seek_offset
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L221-L224
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_seek_stream_response
---

# Signature

`pub(in crate::mapi) fn stream_seek_offset(&self) -> Option<i64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_seek_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_seek_stream_response.md)