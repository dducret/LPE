---
type: Rust Function
title: contact_affected_principals_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L1815-L1840
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update
  - functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx
---

# Signature

`async fn contact_affected_principals_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, contact_book_id: Uuid, ) -> Result<Vec<Uuid>>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [commit_mapi_contact_update](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update.md)
- [record_contact_change_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx.md)