---
type: Rust Method
title: commit_mapi_navigation_shortcut_create
resource: crates/lpe-exchange/src/tests/mod.rs#L9719-L9741
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_create_preserves_distinct_rows_for_same_target
---

# Signature

`fn commit_mapi_navigation_shortcut_create<'a>( &'a self, mut input: crate::store::CommitMapiNavigationShortcutCreateInput, ) -> StoreFuture<'a, crate::store::MapiNavigationShortcutCommit>`

# Calls

- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [upsert_mapi_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut.md)

# Called by

- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql.md)
- [mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql.md)
- [mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql.md)
- [mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink.md)
- [mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql.md)
- [mapi_navigation_shortcut_create_preserves_distinct_rows_for_same_target](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_navigation_shortcut_create_preserves_distinct_rows_for_same_target.md)