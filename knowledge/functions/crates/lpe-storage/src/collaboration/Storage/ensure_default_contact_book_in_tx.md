---
type: Rust Method
title: ensure_default_contact_book_in_tx
resource: crates/lpe-storage/src/collaboration.rs#L1480-L1487
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/ensure_contact_book_in_tx
  called_by:
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
---

# Signature

`pub(crate) async fn ensure_default_contact_book_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, ) -> Result<Uuid>`

# Calls

- [ensure_contact_book_in_tx](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_contact_book_in_tx.md)

# Called by

- [upsert_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)