---
type: Rust Function
title: update_contact_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L1145-L1224
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
---

# Signature

`async fn update_contact_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, contact_book_id: Uuid, contact_id: Uuid, contact: &NormalizedContact, last_modification_time: u64, ) -> Result<()>`

# Calls

- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)