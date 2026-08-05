---
type: Rust Function
title: append_copy_to_stream_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L1065-L1106
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle
  - functions/crates/lpe-exchange/src/mapi/properties/streams/copy_stream
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_size
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_copy_to_stream_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response
---

# Signature

`pub(super) fn append_copy_to_stream_response( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [resolve_writable_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle.md)
- [move_copy_target_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle.md)
- [copy_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/copy_stream.md)
- [stream_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_size.md)
- [rop_copy_to_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_copy_to_stream_response.md)

# Called by

- [append_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response.md)