---
type: Rust Method
title: rebuild_mail_search_document_in_tx
resource: crates/lpe-storage/src/recoverable_items.rs#L370-L435
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/recoverable_items/Storage/restore_recoverable_item
---

# Signature

`async fn rebuild_mail_search_document_in_tx( &self, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: &Uuid, account_id: Uuid, mailbox_message_id: Uuid, message_id: Uuid, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [restore_recoverable_item](../../../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/restore_recoverable_item.md)