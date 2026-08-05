---
type: Rust Function
title: commit_mapi_associated_config_create_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/associated_config_create.rs#L1-L87
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_identity_material_for_store_replica
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/upsert_mapi_associated_config_in_tx
---

# Signature

`async fn commit_mapi_associated_config_create_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, mut input: UpsertMapiAssociatedConfigInput, ) -> Result<MapiAssociatedConfigCommit>`

# Calls

- [mapi_store_identity_for_account_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx.md)
- [allocate_next_mapi_global_counter](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter.md)
- [mapi_identity_material_for_store_replica](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_identity_material_for_store_replica.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [upsert_mapi_associated_config_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/upsert_mapi_associated_config_in_tx.md)