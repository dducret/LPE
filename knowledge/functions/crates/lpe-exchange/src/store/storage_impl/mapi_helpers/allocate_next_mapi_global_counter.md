---
type: Rust Function
title: allocate_next_mapi_global_counter
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L140-L149
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/tombstone_unknown_mapi_navigation_shortcut_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx
---

# Signature

`async fn allocate_next_mapi_global_counter( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, ) -> Result<u64>`

# Calls

- [mapi_store_identity_for_account_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)

# Called by

- [commit_mapi_associated_config_create_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx.md)
- [commit_mapi_associated_config_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx.md)
- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)
- [commit_mapi_navigation_shortcut_create_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx.md)
- [tombstone_unknown_mapi_navigation_shortcut_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/tombstone_unknown_mapi_navigation_shortcut_in_tx.md)
- [commit_mapi_navigation_shortcut_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx.md)