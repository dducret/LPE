---
type: Rust Function
title: append_open_attachment_response
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L247-L389
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/open_attachment_flags_are_valid
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/attach_num
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/pending_event_attachment_object
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_attachment_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response
---

# Signature

`pub(super) fn append_open_attachment_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [open_attachment_flags_are_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/open_attachment_flags_are_valid.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [attach_num](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/attach_num.md)
- [event_attachments_for_parent_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [pending_event_attachment_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/pending_event_attachment_object.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_open_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_attachment_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [attachment_for_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/attachment_for_message.md)

# Called by

- [append_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response.md)