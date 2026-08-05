---
type: Rust Function
title: append_commit_stream_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L1279-L1349
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_associated_config_stream_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/stream_dispatch/append_stream_dispatch_response
---

# Signature

`pub(super) async fn append_commit_stream_response<S>( _store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [resolve_writable_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle.md)
- [is_inbox_associated_config_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_associated_config_stream_handle.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_stream_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/stream_dispatch/append_stream_dispatch_response.md)