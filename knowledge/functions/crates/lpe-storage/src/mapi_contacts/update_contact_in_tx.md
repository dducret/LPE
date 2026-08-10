---
type: Rust Function
title: update_contact_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L1457-L1538
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update
---

# Signature

`async fn update_contact_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, contact_book_id: Uuid, contact_id: Uuid, contact: &NormalizedContact, preserve_import_source: bool, last_modification_time: u64, ) -> Result<()>`

# Calls

- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [commit_mapi_contact_update](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/commit_mapi_contact_update.md)