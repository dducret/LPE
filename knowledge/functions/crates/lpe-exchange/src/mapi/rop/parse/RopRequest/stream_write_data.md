---
type: Rust Method
title: stream_write_data
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L206-L215
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
---

# Signature

`pub(in crate::mapi) fn stream_write_data(&self) -> &[u8]`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_write_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)