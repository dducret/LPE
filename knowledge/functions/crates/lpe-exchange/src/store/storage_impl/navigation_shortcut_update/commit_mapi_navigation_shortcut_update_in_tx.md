---
type: Rust Function
title: commit_mapi_navigation_shortcut_update_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update.rs#L1-L97
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/parse_mapi_predecessor_change_list
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter
  - functions/crates/lpe-storage/src/mapi_store_identity/mapi_xid
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessor_change_key
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/serialize_mapi_predecessor_change_list
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/mapi_predecessors_contain_change_key
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/upsert_mapi_navigation_shortcut_in_tx
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
---

# Signature

`async fn commit_mapi_navigation_shortcut_update_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, tenant_id: Uuid, input: UpsertMapiNavigationShortcutInput, ) -> Result<MapiNavigationShortcutCommit>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [parse_mapi_predecessor_change_list](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/parse_mapi_predecessor_change_list.md)
- [mapi_store_identity_for_account_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_store_identity_for_account_in_tx.md)
- [allocate_next_mapi_global_counter](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_global_counter.md)
- [mapi_xid](../../../../../../../functions/crates/lpe-storage/src/mapi_store_identity/mapi_xid.md)
- [merge_mapi_predecessor_change_key](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessor_change_key.md)
- [serialize_mapi_predecessor_change_list](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/serialize_mapi_predecessor_change_list.md)
- [mapi_predecessors_contain_change_key](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/mapi_predecessors_contain_change_key.md)
- [upsert_mapi_navigation_shortcut_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/upsert_mapi_navigation_shortcut_in_tx.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)