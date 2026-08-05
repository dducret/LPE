---
type: Rust Function
title: append_clone_stream_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L843-L883
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response
---

# Signature

`pub(super) fn append_clone_stream_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [resolve_writable_stream_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/resolve_writable_stream_handle.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_response.md)