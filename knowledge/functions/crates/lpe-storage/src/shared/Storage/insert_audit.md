---
type: Rust Method
title: insert_audit
resource: crates/lpe-storage/src/shared.rs#L674-L695
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/record_platform_audit
  - functions/crates/lpe-storage/src/admin/Storage/create_server_administrator
  - functions/crates/lpe-storage/src/admin/Storage/append_audit_event
  - functions/crates/lpe-storage/src/admin/Storage/put_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/delete_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script
  - functions/crates/lpe-storage/src/admin/Storage/update_settings
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/create_account
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/update_account
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/create_mailbox
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/create_pst_transfer_job
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/create_domain
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/update_domain
  - functions/crates/lpe-storage/src/admin/provisioning/Storage/create_alias
  - functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment
  - functions/crates/lpe-storage/src/auth/Storage/upsert_admin_credential
  - functions/crates/lpe-storage/src/auth/Storage/upsert_account_credential
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_collaboration_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_calendar_collection_grant
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/set_calendar_collection_grant
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
  - functions/crates/lpe-storage/src/mail_items/expunge_imap_deleted
  - functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription
  - functions/crates/lpe-storage/src/mailboxes/Storage/destroy_jmap_mailbox
  - functions/crates/lpe-storage/src/message_ops/Storage/copy_jmap_email
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_tree
  - functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child
  - functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_item
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_item
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_permission
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_replica
  - functions/crates/lpe-storage/src/recoverable_items/Storage/restore_recoverable_item
  - functions/crates/lpe-storage/src/recoverable_items/Storage/purge_recoverable_item
  - functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/replace_storage_policy_assignment
  - functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
  - functions/crates/lpe-storage/src/submission/Storage/cancel_queued_submission
  - functions/crates/lpe-storage/src/submission/Storage/delete_draft_message
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/set_mailbox_folder_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/delete_mailbox_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_sender_delegation_grant
  - functions/crates/lpe-storage/src/submission/delegation/Storage/delete_sender_delegation_grant
  - functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant
  - functions/crates/lpe-storage/src/tasks/Storage/delete_task_list_grant
---

# Signature

`pub(crate) async fn insert_audit( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [record_platform_audit](../../../../../../functions/crates/lpe-storage/src/admin/Storage/record_platform_audit.md)
- [create_server_administrator](../../../../../../functions/crates/lpe-storage/src/admin/Storage/create_server_administrator.md)
- [append_audit_event](../../../../../../functions/crates/lpe-storage/src/admin/Storage/append_audit_event.md)
- [put_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/put_sieve_script.md)
- [delete_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/delete_sieve_script.md)
- [rename_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script.md)
- [set_active_sieve_script](../../../../../../functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script.md)
- [update_settings](../../../../../../functions/crates/lpe-storage/src/admin/Storage/update_settings.md)
- [create_account](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_account.md)
- [update_account](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/update_account.md)
- [create_mailbox](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_mailbox.md)
- [create_pst_transfer_job](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_pst_transfer_job.md)
- [create_domain](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_domain.md)
- [update_domain](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/update_domain.md)
- [create_alias](../../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_alias.md)
- [add_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment.md)
- [delete_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment.md)
- [add_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment.md)
- [delete_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment.md)
- [upsert_admin_credential](../../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_admin_credential.md)
- [upsert_account_credential](../../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_account_credential.md)
- [upsert_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)
- [delete_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_collaboration_grant.md)
- [delete_calendar_collection_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/delete_calendar_collection_grant.md)
- [set_calendar_collection_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/set_calendar_collection_grant.md)
- [deliver_inbound_message](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
- [expunge_imap_deleted](../../../../../../functions/crates/lpe-storage/src/mail_items/expunge_imap_deleted.md)
- [delete_jmap_email_memberships](../../../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships.md)
- [create_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [create_managed_retention_folder](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder.md)
- [create_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)
- [update_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox.md)
- [rename_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox.md)
- [set_mailbox_subscription](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription.md)
- [destroy_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/destroy_jmap_mailbox.md)
- [copy_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/copy_jmap_email.md)
- [move_jmap_email_membership](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership.md)
- [update_jmap_email_followup_flags](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags.md)
- [update_jmap_email_content](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [create_public_folder_tree](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_tree.md)
- [create_public_folder_child](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child.md)
- [update_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder.md)
- [delete_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder.md)
- [upsert_public_folder_item](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_item.md)
- [delete_public_folder_item](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_item.md)
- [upsert_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission.md)
- [delete_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_permission.md)
- [upsert_public_folder_replica](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica.md)
- [delete_public_folder_replica](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_replica.md)
- [restore_recoverable_item](../../../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/restore_recoverable_item.md)
- [purge_recoverable_item](../../../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/purge_recoverable_item.md)
- [create_storage_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool.md)
- [update_storage_pool](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool.md)
- [replace_storage_policy_assignment](../../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/replace_storage_policy_assignment.md)
- [replace_message_recipients](../../../../../../functions/crates/lpe-storage/src/submission/Storage/replace_message_recipients.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)
- [cancel_queued_submission](../../../../../../functions/crates/lpe-storage/src/submission/Storage/cancel_queued_submission.md)
- [delete_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/delete_draft_message.md)
- [upsert_mailbox_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant.md)
- [set_mailbox_folder_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/set_mailbox_folder_delegation_grant.md)
- [delete_mailbox_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/delete_mailbox_delegation_grant.md)
- [upsert_sender_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_sender_delegation_grant.md)
- [delete_sender_delegation_grant](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/delete_sender_delegation_grant.md)
- [upsert_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant.md)
- [delete_task_list_grant](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list_grant.md)