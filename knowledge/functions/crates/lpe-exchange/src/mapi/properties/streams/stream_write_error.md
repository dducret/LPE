---
type: Rust Function
title: stream_write_error
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L622-L634
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
---

# Signature

`pub(in crate::mapi) fn stream_write_error( session: &MapiSession, stream_handle: u32, ) -> Option<StreamWriteError>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_write_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)