---
type: Rust Method
title: commit_mapi_folder_hierarchy_change
resource: crates/lpe-exchange/src/tests/mod.rs#L4943-L5078
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage
---

# Signature

`fn commit_mapi_folder_hierarchy_change<'a>( &'a self, _account_id: Uuid, folder_id: u64, imported_last_modification_time: i64, imported_change_key: &'a [u8], imported_predecessor_change_list: &'a [u8], ) -> StoreFuture<'a, MapiFolderHierarchyCommitOutcome>`

# Calls

- [test_mapi_pcl_includes_change_key](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [test_merge_mapi_predecessor_change_lists](../../../../../../../functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage.md)