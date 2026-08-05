---
type: Rust Function
title: rop_write_stream_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L245-L254
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
---

# Signature

`pub(in crate::mapi) fn rop_write_stream_response(request: &RopRequest, written: usize) -> Vec<u8>`

# Called by

- [append_write_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)