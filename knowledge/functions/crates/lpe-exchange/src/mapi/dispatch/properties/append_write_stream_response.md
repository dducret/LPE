---
type: Rust Function
title: append_write_stream_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L1002-L1063
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_associated_config_stream_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/properties/streams/write_stream
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_write_data
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_write_stream_response
  - functions/crates/lpe-exchange/src/mapi/properties/streams/stream_write_error_code
  - functions/crates/lpe-exchange/src/mapi/properties/streams/stream_write_error
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response
---

# Signature

`pub(super) fn append_write_stream_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, responses: &mut Vec<u8>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [resolve_writable_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle.md)
- [is_inbox_associated_config_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_associated_config_stream_handle.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [write_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/write_stream.md)
- [stream_write_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/stream_write_data.md)
- [rop_write_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_write_stream_response.md)
- [stream_write_error_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/stream_write_error_code.md)
- [stream_write_error](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/stream_write_error.md)

# Called by

- [append_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response.md)