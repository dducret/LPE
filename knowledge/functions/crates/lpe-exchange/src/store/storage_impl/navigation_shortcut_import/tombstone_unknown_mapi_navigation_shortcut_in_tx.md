---
type: Rust Function
title: tombstone_unknown_mapi_navigation_shortcut_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import.rs#L284-L359
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/preflight_unknown_mapi_navigation_shortcut_delete_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_identity_material_for_store_replica
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`async fn tombstone_unknown_mapi_navigation_shortcut_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, folder_id: u64, source_key: &[u8], ) -> Result<()>`

# Calls

- [preflight_unknown_mapi_navigation_shortcut_delete_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/preflight_unknown_mapi_navigation_shortcut_delete_in_tx.md)
- [allocate_next_mapi_global_counter](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter.md)
- [mapi_identity_material_for_store_replica](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_identity_material_for_store_replica.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)