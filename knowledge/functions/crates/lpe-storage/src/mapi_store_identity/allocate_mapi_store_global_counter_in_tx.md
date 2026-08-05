---
type: Rust Function
title: allocate_mapi_store_global_counter_in_tx
resource: crates/lpe-storage/src/mapi_store_identity.rs#L115-L135
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter
  - functions/crates/lpe-storage/src/collaboration/deleted_events/allocate_global_counter_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/allocate_next_contact_change_number_in_tx
  - functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx
  - functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx
---

# Signature

`pub async fn allocate_mapi_store_global_counter_in_tx( tx: &mut Transaction<'_, Postgres>, ) -> Result<(MapiStoreIdentity, u64)>`

# Calls

- [ensure_mapi_store_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [allocate_next_mapi_global_counter](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter.md)
- [allocate_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/allocate_global_counter_in_tx.md)
- [allocate_next_contact_change_number_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/allocate_next_contact_change_number_in_tx.md)
- [rotate_mapi_event_identities_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx.md)
- [allocate_mapi_event_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx.md)
- [rotate_active_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx.md)
- [rekey_active_mapi_message_identity_for_server_move_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx.md)
- [rekey_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx.md)