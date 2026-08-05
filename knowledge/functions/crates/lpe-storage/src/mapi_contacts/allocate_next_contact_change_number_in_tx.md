---
type: Rust Function
title: allocate_next_contact_change_number_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L857-L862
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx
---

# Signature

`async fn allocate_next_contact_change_number_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, ) -> Result<u64>`

# Calls

- [allocate_mapi_store_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)

# Called by

- [commit_existing_contact_import_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/commit_existing_contact_import_in_tx.md)
- [allocate_contact_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_contact_identity_in_tx.md)