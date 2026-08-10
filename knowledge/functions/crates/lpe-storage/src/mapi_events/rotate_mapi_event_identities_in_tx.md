---
type: Rust Function
title: rotate_mapi_event_identities_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L1248-L1347
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx
  - functions/crates/lpe-storage/src/mapi_events/rotate_active_mapi_event_identities_in_tx
---

# Signature

`async fn rotate_mapi_event_identities_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, event_id: Uuid, object_kind: &str, imported_principal_account_id: Option<Uuid>, imported_identity: Option<&MapiEventImportedIdentity>, ) -> Result<Vec<EventIdentityVersion>>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)
- [mapi_change_key](../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [merge_predecessor_change_list](../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [advance_mapi_event_version_for_lifecycle_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx.md)
- [rotate_active_mapi_event_identities_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/rotate_active_mapi_event_identities_in_tx.md)