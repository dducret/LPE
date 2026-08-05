---
type: Rust Function
title: append_synchronization_import_deletes_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes.rs#L14-L325
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_flags
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_message_ids
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_hard_delete
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/preflight_unknown_mapi_navigation_shortcut_deletes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_associated_config
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_checkpoint
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/tombstone_unknown_mapi_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_client_local_message_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_note
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_journal_entry
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/mailbox_is_trash_or_descendant
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response
---

# Signature

`pub(super) async fn append_synchronization_import_deletes_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [import_delete_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_flags.md)
- [synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/synchronization_import_deletes_response.md)
- [import_delete_message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_message_ids.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [import_delete_hard_delete](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_hard_delete.md)
- [import_delete_source_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys.md)
- [source_key_global_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter.md)
- [associated_config_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [associated_config_message_for_folder_and_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_folder_and_source_key.md)
- [navigation_shortcut_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_messages.md)
- [navigation_shortcut_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id.md)
- [preflight_unknown_mapi_navigation_shortcut_deletes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/preflight_unknown_mapi_navigation_shortcut_deletes.md)
- [delete_mapi_associated_config](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_associated_config.md)
- [record_sync_upload_content_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_checkpoint.md)
- [delete_mapi_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut.md)
- [tombstone_unknown_mapi_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/tombstone_unknown_mapi_navigation_shortcut.md)
- [transient_client_local_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_client_local_message_id.md)
- [note_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [delete_mapi_note](../../../../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_note.md)
- [journal_entry_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [delete_mapi_journal_entry](../../../../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_journal_entry.md)
- [mailbox_is_trash_or_descendant](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/mailbox_is_trash_or_descendant.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [canonical_message_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number.md)

# Called by

- [append_sync_import_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response.md)