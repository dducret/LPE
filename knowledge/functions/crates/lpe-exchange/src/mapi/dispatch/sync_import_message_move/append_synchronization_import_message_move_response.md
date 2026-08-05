---
type: Rust Function
title: append_synchronization_import_message_move_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move.rs#L3-L255
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_checkpoint
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_import_message_move_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/completed_message_move_replay_identity
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response
---

# Signature

`pub(super) async fn append_synchronization_import_message_move_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [import_move](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [record_sync_upload_content_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_checkpoint.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [rop_synchronization_import_message_move_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_import_message_move_response.md)
- [note_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [journal_entry_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [completed_message_move_replay_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/completed_message_move_replay_identity.md)

# Called by

- [append_sync_import_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response.md)