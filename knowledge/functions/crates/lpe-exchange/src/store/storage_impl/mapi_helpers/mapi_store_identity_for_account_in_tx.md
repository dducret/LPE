---
type: Rust Function
title: mapi_store_identity_for_account_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L123-L138
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx
  - functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/preflight_unknown_mapi_navigation_shortcut_delete_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx
---

# Signature

`async fn mapi_store_identity_for_account_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, ) -> Result<lpe_storage::MapiStoreIdentity>`

# Calls

- [ensure_mapi_store_identity_in_tx](../../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)

# Called by

- [commit_mapi_associated_config_create_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx.md)
- [commit_mapi_associated_config_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx.md)
- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)
- [allocate_next_mapi_global_counter](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter.md)
- [commit_mapi_navigation_shortcut_create_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_create/commit_mapi_navigation_shortcut_create_in_tx.md)
- [preflight_unknown_mapi_navigation_shortcut_delete_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/preflight_unknown_mapi_navigation_shortcut_delete_in_tx.md)
- [commit_mapi_navigation_shortcut_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx.md)