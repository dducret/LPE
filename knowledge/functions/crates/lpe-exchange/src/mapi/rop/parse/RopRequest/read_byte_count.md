---
type: Rust Method
title: read_byte_count
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L195-L204
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_stream_response
---

# Signature

`pub(in crate::mapi) fn read_byte_count(&self) -> Option<usize>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_read_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_stream_response.md)