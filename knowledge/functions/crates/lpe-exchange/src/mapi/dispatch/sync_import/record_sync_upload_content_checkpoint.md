---
type: Rust Function
title: record_sync_upload_content_checkpoint
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L989-L1013
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response
---

# Signature

`pub(super) fn record_sync_upload_content_checkpoint(session: &mut MapiSession, folder_id: u64)`

# Calls

- [upload_sync_state_stream_from_sets](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets.md)

# Called by

- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [append_synchronization_import_message_move_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response.md)