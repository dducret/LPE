---
type: Rust Method
title: ensure_mailbox
resource: crates/lpe-storage/src/shared.rs#L113-L198
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_imap_mailboxes
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) async fn ensure_mailbox( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, role: &str, display_name: &str, sort_order: i32, _retention_days: i32, ) -> Result<Uuid>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [deliver_inbound_message](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
- [ensure_imap_mailboxes](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_imap_mailboxes.md)
- [create_jmap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [create_imap_mailbox](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)