---
type: Rust Function
title: require_account_from_store
resource: crates/lpe-admin-api/src/workspace.rs#L1350-L1364
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/submit_message_with_store
  - functions/crates/lpe-admin-api/src/workspace/update_message_flag_with_store
  - functions/crates/lpe-admin-api/src/workspace/list_recoverable_items_with_store
  - functions/crates/lpe-admin-api/src/workspace/restore_recoverable_item_with_store
  - functions/crates/lpe-admin-api/src/workspace/purge_recoverable_item_with_store
  - functions/crates/lpe-admin-api/src/workspace/list_client_notes_with_store
  - functions/crates/lpe-admin-api/src/workspace/get_client_note_with_store
  - functions/crates/lpe-admin-api/src/workspace/upsert_client_note_with_store
  - functions/crates/lpe-admin-api/src/workspace/delete_client_note_with_store
  - functions/crates/lpe-admin-api/src/workspace/list_journal_entries_with_store
  - functions/crates/lpe-admin-api/src/workspace/get_journal_entry_with_store
  - functions/crates/lpe-admin-api/src/workspace/upsert_journal_entry_with_store
  - functions/crates/lpe-admin-api/src/workspace/delete_journal_entry_with_store
  - functions/crates/lpe-admin-api/src/workspace/query_client_reminders_with_store
  - functions/crates/lpe-admin-api/src/workspace/list_search_folders_with_store
  - functions/crates/lpe-admin-api/src/workspace/get_search_folder_with_store
  - functions/crates/lpe-admin-api/src/workspace/upsert_search_folder_with_store
  - functions/crates/lpe-admin-api/src/workspace/delete_search_folder_with_store
  - functions/crates/lpe-admin-api/src/workspace/outlook_profile_state_with_store
---

# Signature

`async fn require_account_from_store<S: ClientSessionStore>( storage: &S, headers: &HeaderMap, ) -> std::result::Result<AuthenticatedAccount, (StatusCode, String)>`

# Called by

- [submit_message_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/submit_message_with_store.md)
- [update_message_flag_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/update_message_flag_with_store.md)
- [list_recoverable_items_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/list_recoverable_items_with_store.md)
- [restore_recoverable_item_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/restore_recoverable_item_with_store.md)
- [purge_recoverable_item_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/purge_recoverable_item_with_store.md)
- [list_client_notes_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/list_client_notes_with_store.md)
- [get_client_note_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/get_client_note_with_store.md)
- [upsert_client_note_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_note_with_store.md)
- [delete_client_note_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_client_note_with_store.md)
- [list_journal_entries_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/list_journal_entries_with_store.md)
- [get_journal_entry_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/get_journal_entry_with_store.md)
- [upsert_journal_entry_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_journal_entry_with_store.md)
- [delete_journal_entry_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_journal_entry_with_store.md)
- [query_client_reminders_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/query_client_reminders_with_store.md)
- [list_search_folders_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/list_search_folders_with_store.md)
- [get_search_folder_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/get_search_folder_with_store.md)
- [upsert_search_folder_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_search_folder_with_store.md)
- [delete_search_folder_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/delete_search_folder_with_store.md)
- [outlook_profile_state_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/outlook_profile_state_with_store.md)