---
type: Rust Method
title: commit_mapi_navigation_shortcut_import
resource: crates/lpe-exchange/src/tests/mod.rs#L10015-L10269
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted
---

# Signature

`fn commit_mapi_navigation_shortcut_import<'a>( &'a self, input: crate::store::CommitMapiNavigationShortcutImportInput, ) -> StoreFuture<'a, crate::store::MapiNavigationShortcutImportCommit>`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [test_mapi_pcl_includes_change_key](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [test_merge_mapi_predecessor_change_lists](../../../../../../../functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [upsert_mapi_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut.md)

# Called by

- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges.md)
- [mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink.md)
- [mapi_navigation_shortcut_import_commits_content_and_identity_atomically](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_import_commits_content_and_identity_atomically.md)
- [mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted.md)