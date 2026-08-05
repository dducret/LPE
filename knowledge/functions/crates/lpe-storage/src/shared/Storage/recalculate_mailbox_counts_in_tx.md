---
type: Rust Method
title: recalculate_mailbox_counts_in_tx
resource: crates/lpe-storage/src/shared.rs#L358-L394
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/mail_items/update_imap_flags
  - functions/crates/lpe-storage/src/mail_items/expunge_imap_deleted
  - functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships
  - functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/delete_draft_message_in_tx
---

# Signature

`pub(crate) async fn recalculate_mailbox_counts_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, mailbox_id: Uuid, modseq: i64, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [update_imap_flags](../../../../../../functions/crates/lpe-storage/src/mail_items/update_imap_flags.md)
- [expunge_imap_deleted](../../../../../../functions/crates/lpe-storage/src/mail_items/expunge_imap_deleted.md)
- [delete_jmap_email_memberships](../../../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships.md)
- [move_jmap_email_membership](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership.md)
- [update_jmap_email_followup_flags](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags.md)
- [allocate_mailbox_membership_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mailbox_membership_in_tx.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [delete_draft_message_in_tx](../../../../../../functions/crates/lpe-storage/src/submission/Storage/delete_draft_message_in_tx.md)