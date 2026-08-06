---
type: Rust Method
title: allocate_account_modseq_in_tx
resource: crates/lpe-storage/src/shared.rs#L60-L85
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/put_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script
  - functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_with_reason_in_tx
  - functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_calendar_collection
  - functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_calendar_collection
  - functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event_reminder
  - functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/move_accessible_event_to_deleted_items
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_collaboration_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_calendar_collection_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/set_calendar_collection_grant
  - functions/crates/lpe-storage/src/conversation_actions/Storage/upsert_conversation_action
  - functions/crates/lpe-storage/src/conversation_actions/Storage/delete_conversation_action
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update
  - functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/move_calendar_events_to_collection_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
  - functions/crates/lpe-storage/src/notes_journal/Storage/upsert_client_note
  - functions/crates/lpe-storage/src/notes_journal/Storage/upsert_journal_entry
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_private_change
  - functions/crates/lpe-storage/src/search_folders/Storage/upsert_search_folder
  - functions/crates/lpe-storage/src/search_folders/Storage/ensure_exchange_search_folders
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task
  - functions/crates/lpe-storage/src/tasks/Storage/update_accessible_task_reminder
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant
  - functions/crates/lpe-storage/src/tasks/Storage/delete_task_list_grant
  - functions/crates/lpe-storage/src/tasks/Storage/create_task_list
  - functions/crates/lpe-storage/src/tasks/Storage/update_task_list
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar
---

# Signature

`pub(crate) async fn allocate_account_modseq_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, category: &str, ) -> Result<i64>`

# Called by

- [put_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/put_sieve_script.md)
- [rename_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script.md)
- [set_active_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script.md)
- [add_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment.md)
- [delete_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment.md)
- [insert_collaboration_tombstone_with_reason_in_tx](../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_with_reason_in_tx.md)
- [create_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_calendar_collection.md)
- [update_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_calendar_collection.md)
- [update_accessible_event_reminder](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event_reminder.md)
- [move_accessible_event_to_deleted_items](../../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/move_accessible_event_to_deleted_items.md)
- [upsert_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)
- [delete_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_collaboration_grant.md)
- [delete_calendar_collection_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_calendar_collection_grant.md)
- [set_calendar_collection_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/set_calendar_collection_grant.md)
- [upsert_conversation_action](../../../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/upsert_conversation_action.md)
- [delete_conversation_action](../../../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/delete_conversation_action.md)
- [create_mapi_contact](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [commit_mapi_contact_update](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update.md)
- [record_contact_change_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx.md)
- [move_calendar_events_to_collection_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/move_calendar_events_to_collection_in_tx.md)
- [create_mapi_event](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [commit_mapi_event_update](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)
- [upsert_client_note](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/upsert_client_note.md)
- [upsert_journal_entry](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/upsert_journal_entry.md)
- [record_public_folder_change_with_extra_affected](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected.md)
- [record_public_folder_private_change](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_private_change.md)
- [upsert_search_folder](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/upsert_search_folder.md)
- [ensure_exchange_search_folders](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/ensure_exchange_search_folders.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [upsert_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)
- [update_accessible_task_reminder](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_accessible_task_reminder.md)
- [upsert_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant.md)
- [delete_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list_grant.md)
- [create_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/create_task_list.md)
- [update_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_task_list.md)
- [upsert_client_contact_in_book_role](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)
- [upsert_client_event_in_calendar](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar.md)