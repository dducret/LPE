---
type: Rust Function
title: insert_mapi_associated_config_change
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L456-L493
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/upsert_mapi_associated_config_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/delete_mapi_associated_config_in_tx
---

# Signature

`async fn insert_mapi_associated_config_change( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, config_id: Uuid, change_kind: &str, folder_id: u64, ) -> Result<()>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [upsert_mapi_associated_config_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/upsert_mapi_associated_config_in_tx.md)
- [delete_mapi_associated_config_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/delete_mapi_associated_config_in_tx.md)