---
type: Rust Function
title: append_get_attachment_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L171-L245
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/get_attachment_table_flags_are_valid
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/attachment_table_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_attachment_table_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response
---

# Signature

`pub(super) fn append_get_attachment_table_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [get_attachment_table_flags_are_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/get_attachment_table_flags_are_valid.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [attachment_table_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/attachment_table_object.md)
- [event_attachments_for_parent_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [get_attachment_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_attachment_table_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response.md)