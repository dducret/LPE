---
type: Rust Method
title: insert_mail_change_log_in_tx
resource: crates/lpe-storage/src/shared.rs#L200-L239
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/put_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script
  - functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment
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
  - functions/crates/lpe-storage/src/mail_items/update_imap_flags
  - functions/crates/lpe-storage/src/mail_items/expunge_imap_deleted
  - functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription
  - functions/crates/lpe-storage/src/mailboxes/Storage/destroy_jmap_mailbox
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
  - functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/move_calendar_events_to_collection_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content
  - functions/crates/lpe-storage/src/notes_journal/Storage/upsert_client_note
  - functions/crates/lpe-storage/src/notes_journal/Storage/upsert_journal_entry
  - functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_private_change
  - functions/crates/lpe-storage/src/recoverable_items/Storage/restore_recoverable_item
  - functions/crates/lpe-storage/src/recoverable_items/Storage/purge_recoverable_item
  - functions/crates/lpe-storage/src/search_folders/Storage/upsert_search_folder
  - functions/crates/lpe-storage/src/search_folders/Storage/ensure_exchange_search_folders
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx
  - functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
  - functions/crates/lpe-storage/src/submission/Storage/cancel_queued_submission
  - functions/crates/lpe-storage/src/submission/Storage/delete_draft_message_in_tx
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/set_mailbox_folder_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/delete_mailbox_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_sender_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/delete_sender_delegation_grant
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

`pub(crate) async fn insert_mail_change_log_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Option<Uuid>, mailbox_id: Option<Uuid>, object_kind: &str, object_id: Uuid, change_kind: &str, modseq: i64, affected_principal_ids: &[Uuid], summary_json: Value, ) -> Result<i64>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [put_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/put_sieve_script.md)
- [rename_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script.md)
- [set_active_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script.md)
- [add_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment.md)
- [delete_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment.md)
- [add_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment.md)
- [delete_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment.md)
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
- [update_imap_flags](../../../../../../functions/crates/lpe-storage/src/mail_items/update_imap_flags.md)
- [expunge_imap_deleted](../../../../../../functions/crates/lpe-storage/src/mail_items/expunge_imap_deleted.md)
- [delete_jmap_email_memberships](../../../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships.md)
- [create_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [create_managed_retention_folder](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder.md)
- [create_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)
- [update_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox.md)
- [rename_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox.md)
- [set_mailbox_subscription](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription.md)
- [destroy_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/destroy_jmap_mailbox.md)
- [create_mapi_contact](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [record_contact_change_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx.md)
- [move_calendar_events_to_collection_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/move_calendar_events_to_collection_in_tx.md)
- [create_mapi_event](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [commit_mapi_event_update](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)
- [move_jmap_email_membership](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership.md)
- [update_jmap_email_followup_flags](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags.md)
- [update_jmap_email_content](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content.md)
- [upsert_client_note](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/upsert_client_note.md)
- [upsert_journal_entry](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/upsert_journal_entry.md)
- [update_outbound_queue_status](../../../../../../functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status.md)
- [record_public_folder_change_with_extra_affected](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected.md)
- [record_public_folder_private_change](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_private_change.md)
- [restore_recoverable_item](../../../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/restore_recoverable_item.md)
- [purge_recoverable_item](../../../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/purge_recoverable_item.md)
- [upsert_search_folder](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/upsert_search_folder.md)
- [ensure_exchange_search_folders](../../../../../../functions/crates/lpe-storage/src/search_folders/Storage/ensure_exchange_search_folders.md)
- [allocate_mailbox_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx.md)
- [replace_message_recipients](../../../../../../functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)
- [cancel_queued_submission](../../../../../../functions/crates/lpe-storage/src/submission/Storage/cancel_queued_submission.md)
- [delete_draft_message_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/delete_draft_message_in_tx.md)
- [upsert_mailbox_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant.md)
- [set_mailbox_folder_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/set_mailbox_folder_delegation_grant.md)
- [delete_mailbox_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/delete_mailbox_delegation_grant.md)
- [upsert_sender_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_sender_delegation_grant.md)
- [delete_sender_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/delete_sender_delegation_grant.md)
- [upsert_client_task](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)
- [update_accessible_task_reminder](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_accessible_task_reminder.md)
- [upsert_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant.md)
- [delete_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list_grant.md)
- [create_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/create_task_list.md)
- [update_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_task_list.md)
- [upsert_client_contact_in_book_role](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)
- [upsert_client_event_in_calendar](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar.md)