---
type: Rust Function
title: append_get_valid_attachments_response
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L117-L169
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle
  - functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachment_numbers_response
  - functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachments_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response
---

# Signature

`pub(super) fn append_get_valid_attachments_response( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [event_attachments_for_parent_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle.md)
- [rop_get_valid_attachment_numbers_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachment_numbers_response.md)
- [rop_get_valid_attachments_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachments_response.md)

# Called by

- [append_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response.md)