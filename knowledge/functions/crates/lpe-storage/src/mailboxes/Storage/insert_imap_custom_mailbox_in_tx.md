---
type: Rust Method
title: insert_imap_custom_mailbox_in_tx
resource: crates/lpe-storage/src/mailboxes.rs#L338-L394
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription_in_tx
  called_by:
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox
---

# Signature

`async fn insert_imap_custom_mailbox_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, parent_id: Option<Uuid>, display_name: &str, ) -> Result<(Uuid, i64)>`

# Calls

- [ensure_mailbox_name_available_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [set_mailbox_subscription_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription_in_tx.md)

# Called by

- [create_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)
- [rename_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox.md)