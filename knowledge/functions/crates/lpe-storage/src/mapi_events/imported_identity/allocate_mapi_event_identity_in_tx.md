---
type: Rust Function
title: allocate_mapi_event_identity_in_tx
resource: crates/lpe-storage/src/mapi_events/imported_identity.rs#L52-L149
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_source_global_counter
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
---

# Signature

`pub(super) async fn allocate_mapi_event_identity_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, principal_account_id: Uuid, event_id: Uuid, imported_identity: Option<&MapiEventImportedIdentity>, ) -> Result<(u64, EventIdentityVersion)>`

# Calls

- [ensure_mapi_store_identity_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)
- [imported_source_global_counter](../../../../../../functions/crates/lpe-storage/src/mapi_events/imported_identity/imported_source_global_counter.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)
- [mapi_change_key](../../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [merge_predecessor_change_list](../../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [create_mapi_event](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)