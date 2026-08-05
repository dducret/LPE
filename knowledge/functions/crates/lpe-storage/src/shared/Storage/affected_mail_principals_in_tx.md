---
type: Rust Method
title: affected_mail_principals_in_tx
resource: crates/lpe-storage/src/shared.rs#L241-L262
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment
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
---

# Signature

`pub(crate) async fn affected_mail_principals_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, ) -> Result<Vec<Uuid>>`

# Called by

- [add_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment.md)
- [delete_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment.md)
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