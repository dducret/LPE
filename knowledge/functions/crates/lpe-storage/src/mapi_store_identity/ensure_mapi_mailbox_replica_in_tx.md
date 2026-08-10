---
type: Rust Function
title: ensure_mapi_mailbox_replica_in_tx
resource: crates/lpe-storage/src/mapi_store_identity.rs#L76-L113
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx
  - functions/crates/lpe-storage/src/collaboration/deleted_events/allocate_global_counter_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/rotate_active_mapi_contact_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_contacts/lock_contact_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx
  - functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx
---

# Signature

`pub async fn ensure_mapi_mailbox_replica_in_tx( tx: &mut Transaction<'_, Postgres>, tenant_id: Uuid, account_id: Uuid, store_identity: MapiStoreIdentity, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [mapi_store_identity_for_account_in_tx](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx.md)
- [allocate_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/allocate_global_counter_in_tx.md)
- [rotate_active_mapi_contact_identities_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/rotate_active_mapi_contact_identities_in_tx.md)
- [lock_contact_replica_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_contacts/lock_contact_replica_in_tx.md)
- [rotate_mapi_event_identities_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx.md)
- [allocate_mapi_event_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/allocate_mapi_event_identity_in_tx.md)
- [rotate_active_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx.md)
- [rekey_active_mapi_message_identity_for_server_move_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx.md)
- [rekey_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx.md)