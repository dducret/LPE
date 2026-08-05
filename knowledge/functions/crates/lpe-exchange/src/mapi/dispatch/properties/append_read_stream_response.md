---
type: Rust Function
title: append_read_stream_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L710-L841
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_rule_organizer_stream_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_associated_config_stream_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_stream_read
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_stream_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_rule_organizer_stream_read
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response
---

# Signature

`pub(super) fn append_read_stream_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, responses: &mut Vec<u8>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [resolve_writable_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle.md)
- [is_inbox_rule_organizer_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_rule_organizer_stream_handle.md)
- [is_inbox_associated_config_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/is_inbox_associated_config_stream_handle.md)
- [record_inbox_associated_config_stream_read](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_config_stream_read.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [rop_read_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_stream_response.md)
- [record_inbox_rule_organizer_stream_read](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_rule_organizer_stream_read.md)

# Called by

- [append_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response.md)