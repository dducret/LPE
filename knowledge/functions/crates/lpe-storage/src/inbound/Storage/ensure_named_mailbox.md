---
type: Rust Method
title: ensure_named_mailbox
resource: crates/lpe-storage/src/inbound.rs#L567-L626
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
---

# Signature

`async fn ensure_named_mailbox( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, display_name: &str, _retention_days: i32, ) -> Result<Uuid>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [collides_with](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with.md)

# Called by

- [deliver_inbound_message](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)