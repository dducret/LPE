---
type: Rust Method
title: delete_draft_message_in_tx
resource: crates/lpe-storage/src/submission.rs#L1416-L1528
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx
  called_by:
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
  - functions/crates/lpe-storage/src/submission/Storage/delete_draft_message
---

# Signature

`async fn delete_draft_message_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, message_id: Uuid, ) -> Result<()>`

# Calls

- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [recalculate_mailbox_counts_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx.md)

# Called by

- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)
- [delete_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/delete_draft_message.md)