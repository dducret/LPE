---
type: Rust Method
title: allocate_mail_modseq_in_tx
resource: crates/lpe-storage/src/shared.rs#L50-L58
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment
  - functions/crates/lpe-storage/src/mail_items/update_imap_flags
  - functions/crates/lpe-storage/src/mail_items/expunge_imap_deleted
  - functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships
  - functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder
  - functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription
  - functions/crates/lpe-storage/src/mailboxes/Storage/destroy_jmap_mailbox
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content
  - functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status
  - functions/crates/lpe-storage/src/recoverable_items/Storage/restore_recoverable_item
  - functions/crates/lpe-storage/src/recoverable_items/Storage/purge_recoverable_item
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
---

# Signature

`pub(crate) async fn allocate_mail_modseq_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, ) -> Result<i64>`

# Calls

- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)

# Called by

- [add_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment.md)
- [delete_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment.md)
- [update_imap_flags](../../../../../../functions/crates/lpe-storage/src/mail_items/update_imap_flags.md)
- [expunge_imap_deleted](../../../../../../functions/crates/lpe-storage/src/mail_items/expunge_imap_deleted.md)
- [delete_jmap_email_memberships](../../../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships.md)
- [insert_imap_custom_mailbox_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx.md)
- [create_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [create_managed_retention_folder](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder.md)
- [update_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox.md)
- [rename_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox.md)
- [set_mailbox_subscription](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription.md)
- [destroy_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/destroy_jmap_mailbox.md)
- [move_jmap_email_membership](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership.md)
- [update_jmap_email_followup_flags](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags.md)
- [update_jmap_email_content](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content.md)
- [update_outbound_queue_status](../../../../../../functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status.md)
- [restore_recoverable_item](../../../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/restore_recoverable_item.md)
- [purge_recoverable_item](../../../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/purge_recoverable_item.md)
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