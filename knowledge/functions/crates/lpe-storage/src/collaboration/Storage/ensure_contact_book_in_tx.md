---
type: Rust Method
title: ensure_contact_book_in_tx
resource: crates/lpe-storage/src/collaboration.rs#L1489-L1519
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_contact_book_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
---

# Signature

`pub(crate) async fn ensure_contact_book_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, role: &str, ) -> Result<Uuid>`

# Called by

- [ensure_default_contact_book_in_tx](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_contact_book_in_tx.md)
- [create_mapi_contact](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [upsert_client_contact_in_book_role](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)