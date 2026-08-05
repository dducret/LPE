---
type: Rust Function
title: append_upload_state_stream_begin_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_upload_state.rs#L3-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/upload_state_property_tag
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/upload_state_transfer_size
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_upload_state_success_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response
---

# Signature

`pub(super) fn append_upload_state_stream_begin_response( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailbox_email: &str, request_id: &str, responses: &mut Vec<u8>, )`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [upload_state_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/upload_state_property_tag.md)
- [upload_state_transfer_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/upload_state_transfer_size.md)
- [rop_upload_state_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_upload_state_success_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_sync_transfer_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response.md)