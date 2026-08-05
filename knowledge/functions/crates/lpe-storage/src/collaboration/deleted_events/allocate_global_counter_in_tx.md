---
type: Rust Function
title: allocate_global_counter_in_tx
resource: crates/lpe-storage/src/collaboration/deleted_events.rs#L433-L447
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  called_by:
  - functions/crates/lpe-storage/src/collaboration/deleted_events/rekey_active_event_identities_in_tx
---

# Signature

`async fn allocate_global_counter_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, ) -> Result<(Uuid, u64)>`

# Calls

- [ensure_mapi_store_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)

# Called by

- [rekey_active_event_identities_in_tx](../../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/rekey_active_event_identities_in_tx.md)