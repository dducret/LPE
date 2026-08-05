---
type: Rust Method
title: stream_size
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L226-L229
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response
---

# Signature

`pub(in crate::mapi) fn stream_size(&self) -> Option<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_set_stream_size_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response.md)
- [append_copy_to_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response.md)