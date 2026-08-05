---
type: Rust Function
title: rop_seek_stream_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L213-L243
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_seek_offset
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_seek_origin
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_seek_stream_response
---

# Signature

`pub(in crate::mapi) fn rop_seek_stream_response( request: &RopRequest, stream: &mut MapiObject, ) -> Vec<u8>`

# Calls

- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [stream_seek_offset](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_seek_offset.md)
- [stream_seek_origin](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_seek_origin.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)

# Called by

- [append_seek_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_seek_stream_response.md)