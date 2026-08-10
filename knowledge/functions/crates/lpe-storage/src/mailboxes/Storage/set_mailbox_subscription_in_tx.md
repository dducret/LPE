---
type: Rust Method
title: set_mailbox_subscription_in_tx
resource: crates/lpe-storage/src/mailboxes.rs#L438-L466
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder
  - functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription
---

# Signature

`async fn set_mailbox_subscription_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, mailbox_account_id: Uuid, mailbox_id: Uuid, subscriber_account_id: Uuid, is_subscribed: bool, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [insert_imap_custom_mailbox_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx.md)
- [create_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [create_managed_retention_folder](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder.md)
- [update_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox.md)
- [set_mailbox_subscription](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription.md)