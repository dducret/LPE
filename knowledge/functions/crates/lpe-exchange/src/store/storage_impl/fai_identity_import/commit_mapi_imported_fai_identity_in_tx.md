---
type: Rust Function
title: commit_mapi_imported_fai_identity_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/fai_identity_import.rs#L30-L343
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_xid_global_counter
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/parse_mapi_predecessor_change_list
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/serialize_mapi_predecessor_change_list
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/mapi_predecessors_contain_change_key
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/mapi_predecessors_include
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessors
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessor_change_key
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/imported_fai_version_wins_last_writer
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_replica_ids/mapi_local_replica_counter_is_deleted_in_folder_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_replica_ids/mapi_local_replica_counter_is_reserved_in_tx
---

# Signature

`async fn commit_mapi_imported_fai_identity_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, account_id: Uuid, object_kind: MapiIdentityObjectKind, folder_id: u64, requested_canonical_id: Option<Uuid>, imported: &MapiFaiImportedIdentity, fail_on_conflict: bool, ) -> Result<MapiImportedFaiIdentityCommit>`

# Calls

- [mapi_store_identity_for_account_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mapi_xid_global_counter](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_xid_global_counter.md)
- [parse_mapi_predecessor_change_list](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/parse_mapi_predecessor_change_list.md)
- [serialize_mapi_predecessor_change_list](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/serialize_mapi_predecessor_change_list.md)
- [mapi_predecessors_contain_change_key](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/mapi_predecessors_contain_change_key.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [mapi_predecessors_include](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/mapi_predecessors_include.md)
- [allocate_next_mapi_global_counter](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter.md)
- [merge_mapi_predecessors](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessors.md)
- [merge_mapi_predecessor_change_key](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessor_change_key.md)
- [imported_fai_version_wins_last_writer](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/imported_fai_version_wins_last_writer.md)
- [mapi_local_replica_counter_is_deleted_in_folder_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_replica_ids/mapi_local_replica_counter_is_deleted_in_folder_in_tx.md)
- [mapi_local_replica_counter_is_reserved_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_replica_ids/mapi_local_replica_counter_is_reserved_in_tx.md)