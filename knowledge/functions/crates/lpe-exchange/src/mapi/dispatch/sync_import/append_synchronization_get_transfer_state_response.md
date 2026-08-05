---
type: Rust Function
title: append_synchronization_get_transfer_state_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L342-L457
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/session/synchronization_context_state
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/sync/sync_emails_for
  - functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_versions
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_attachments
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_get_transfer_state_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response
---

# Signature

`pub(super) fn append_synchronization_get_transfer_state_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [synchronization_context_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/synchronization_context_state.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [upload_sync_state_stream_from_sets](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets.md)
- [sync_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)
- [sync_emails_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_emails_for.md)
- [sync_attachment_facts_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for.md)
- [folder_versions](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_versions.md)
- [sync_state_token_with_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_attachments.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_synchronization_get_transfer_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_get_transfer_state_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_sync_transfer_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response.md)