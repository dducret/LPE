---
type: Rust Function
title: ensure_mapi_store_identity_in_tx
resource: crates/lpe-storage/src/mapi_store_identity.rs#L55-L74
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx
  - functions/crates/lpe-storage/src/collaboration/deleted_events/allocate_global_counter_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/lock_contact_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/Storage/fetch_mapi_store_identity
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/reserve_mapi_store_global_counter_range_in_tx
  - functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx
---

# Signature

`pub async fn ensure_mapi_store_identity_in_tx( tx: &mut Transaction<'_, Postgres>, ) -> Result<MapiStoreIdentity>`

# Called by

- [mapi_store_identity_for_account_in_tx](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx.md)
- [allocate_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/allocate_global_counter_in_tx.md)
- [lock_contact_replica_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/lock_contact_replica_in_tx.md)
- [allocate_mapi_event_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx.md)
- [fetch_mapi_store_identity](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/Storage/fetch_mapi_store_identity.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)
- [reserve_mapi_store_global_counter_range_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/reserve_mapi_store_global_counter_range_in_tx.md)
- [rekey_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx.md)