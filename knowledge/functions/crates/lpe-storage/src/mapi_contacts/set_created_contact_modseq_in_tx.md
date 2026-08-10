---
type: Rust Function
title: set_created_contact_modseq_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L1768-L1813
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update
  - functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx
---

# Signature

`async fn set_created_contact_modseq_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, contact_book_id: Uuid, contact_id: Uuid, modseq: i64, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [commit_mapi_contact_update](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update.md)
- [record_contact_change_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx.md)