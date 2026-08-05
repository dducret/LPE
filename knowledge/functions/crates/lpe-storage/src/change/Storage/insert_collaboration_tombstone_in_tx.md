---
type: Rust Method
title: insert_collaboration_tombstone_in_tx
resource: crates/lpe-storage/src/change.rs#L461-L486
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_with_reason_in_tx
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/delete_sieve_script
  - functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection
  - functions/crates/lpe-storage/src/message_ops/Storage/delete_client_contact
  - functions/crates/lpe-storage/src/notes_journal/Storage/delete_client_note
  - functions/crates/lpe-storage/src/notes_journal/Storage/delete_journal_entry
  - functions/crates/lpe-storage/src/search_folders/Storage/delete_search_folder
  - functions/crates/lpe-storage/src/tasks/Storage/delete_task_list
  - functions/crates/lpe-storage/src/tasks/Storage/delete_client_task
---

# Signature

`pub(crate) async fn insert_collaboration_tombstone_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, category: CanonicalChangeCategory, owner_account_id: Uuid, collection_id: Option<Uuid>, object_kind: &str, object_id: Uuid, object_uid: Option<&str>, affected_principal_ids: &[Uuid], ) -> Result<()>`

# Calls

- [insert_collaboration_tombstone_with_reason_in_tx](../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_with_reason_in_tx.md)

# Called by

- [delete_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/delete_sieve_script.md)
- [delete_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection.md)
- [delete_client_contact](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/delete_client_contact.md)
- [delete_client_note](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/delete_client_note.md)
- [delete_journal_entry](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/delete_journal_entry.md)
- [delete_search_folder](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/delete_search_folder.md)
- [delete_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list.md)
- [delete_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_client_task.md)