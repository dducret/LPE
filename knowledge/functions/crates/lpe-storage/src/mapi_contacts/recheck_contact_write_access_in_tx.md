---
type: Rust Function
title: recheck_contact_write_access_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L830-L874
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
---

# Signature

`async fn recheck_contact_write_access_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, contact_book_id: Uuid, principal_account_id: Uuid, ) -> Result<()>`

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)