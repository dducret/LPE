---
type: Rust Function
title: allocate_contact_identity_in_tx
resource: crates/lpe-storage/src/mapi_contacts.rs#L864-L1015
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_contacts/lock_contact_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/imported_source_counter
  - functions/crates/lpe-storage/src/mapi_contacts/allocate_next_contact_change_number_in_tx
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
---

# Signature

`async fn allocate_contact_identity_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, principal_account_id: Uuid, mapi_folder_id: u64, contact_id: Uuid, imported_identity: Option<&MapiContactImportedIdentity>, last_modification_time: u64, ) -> Result<AllocatedContactIdentity>`

# Calls

- [lock_contact_replica_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/lock_contact_replica_in_tx.md)
- [imported_source_counter](../../../../../functions/crates/lpe-storage/src/mapi_contacts/imported_source_counter.md)
- [allocate_next_contact_change_number_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_next_contact_change_number_in_tx.md)
- [mapi_change_key](../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [merge_predecessor_change_list](../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [create_mapi_contact](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)