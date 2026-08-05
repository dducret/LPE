---
type: Rust Function
title: append_fast_transfer_source_copy_messages_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L200-L265
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_message_ids
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  - functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_list_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_fast_transfer_source_copy_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response
---

# Signature

`pub(super) fn append_fast_transfer_source_copy_messages_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [fast_transfer_message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_message_ids.md)
- [emails_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)
- [sync_attachment_facts_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for.md)
- [fast_transfer_message_list_buffer_with_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_list_buffer_with_attachments.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_fast_transfer_source_copy_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_fast_transfer_source_copy_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_sync_transfer_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response.md)