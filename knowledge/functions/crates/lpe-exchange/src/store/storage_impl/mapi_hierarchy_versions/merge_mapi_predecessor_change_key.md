---
type: Rust Function
title: merge_mapi_predecessor_change_key
resource: crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions.rs#L301-L311
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessors
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/predecessor_merge_is_idempotent_and_keeps_both_replicas
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/stale_advertised_fid_change_key_is_a_conflict_with_durable_server_version
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx
---

# Signature

`fn merge_mapi_predecessor_change_key( entries: &mut MapiPredecessors, change_key: &[u8], ) -> Result<()>`

# Calls

- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [merge_mapi_predecessors](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessors.md)

# Called by

- [commit_mapi_associated_config_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx.md)
- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)
- [predecessor_merge_is_idempotent_and_keeps_both_replicas](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/predecessor_merge_is_idempotent_and_keeps_both_replicas.md)
- [stale_advertised_fid_change_key_is_a_conflict_with_durable_server_version](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/stale_advertised_fid_change_key_is_a_conflict_with_durable_server_version.md)
- [commit_mapi_navigation_shortcut_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx.md)