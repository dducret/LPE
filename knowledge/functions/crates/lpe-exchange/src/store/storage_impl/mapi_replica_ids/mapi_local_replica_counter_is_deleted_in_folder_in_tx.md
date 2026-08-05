---
type: Rust Function
title: mapi_local_replica_counter_is_deleted_in_folder_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/mapi_replica_ids.rs#L198-L231
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
---

# Signature

`async fn mapi_local_replica_counter_is_deleted_in_folder_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, folder_id: u64, replica_guid: Uuid, global_counter: u64, ) -> Result<bool>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)