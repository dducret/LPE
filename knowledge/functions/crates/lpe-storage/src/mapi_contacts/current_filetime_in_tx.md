---
type: Rust Function
title: current_filetime_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L812-L828
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update
---

# Signature

`async fn current_filetime_in_tx(tx: &mut sqlx::Transaction<'_, Postgres>) -> Result<u64>`

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [commit_mapi_contact_update](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update.md)