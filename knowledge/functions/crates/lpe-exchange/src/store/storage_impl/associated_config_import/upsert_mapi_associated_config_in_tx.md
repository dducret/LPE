---
type: Rust Function
title: upsert_mapi_associated_config_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/associated_config_import.rs#L36-L136
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_associated_config_from_row
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/insert_mapi_associated_config_change
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx
---

# Signature

`async fn upsert_mapi_associated_config_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, input: UpsertMapiAssociatedConfigInput, creation_time: Option<u64>, ) -> Result<MapiAssociatedConfigRecord>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [mapi_associated_config_from_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_associated_config_from_row.md)
- [insert_mapi_associated_config_change](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/insert_mapi_associated_config_change.md)

# Called by

- [commit_mapi_associated_config_create_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_create/commit_mapi_associated_config_create_in_tx.md)
- [commit_mapi_associated_config_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx.md)