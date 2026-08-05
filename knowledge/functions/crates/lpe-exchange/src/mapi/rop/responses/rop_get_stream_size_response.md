---
type: Rust Function
title: rop_get_stream_size_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L268-L276
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_stream_size_response
---

# Signature

`pub(in crate::mapi) fn rop_get_stream_size_response( request: &RopRequest, stream_size: usize, ) -> Vec<u8>`

# Called by

- [append_get_stream_size_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_stream_size_response.md)