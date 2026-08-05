---
type: Rust Function
title: lock_contact_replica_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L847-L855
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx
---

# Signature

`async fn lock_contact_replica_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, principal_account_id: Uuid, ) -> Result<Uuid>`

# Calls

- [ensure_mapi_store_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)

# Called by

- [commit_existing_contact_import_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)
- [allocate_contact_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx.md)