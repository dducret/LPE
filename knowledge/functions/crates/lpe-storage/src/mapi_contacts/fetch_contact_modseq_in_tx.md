---
type: Rust Function
title: fetch_contact_modseq_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L1638-L1662
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
---

# Signature

`async fn fetch_contact_modseq_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, contact_book_id: Uuid, contact_id: Uuid, ) -> Result<i64>`

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)