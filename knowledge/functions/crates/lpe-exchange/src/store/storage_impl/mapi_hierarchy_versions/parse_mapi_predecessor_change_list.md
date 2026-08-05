---
type: Rust Function
title: parse_mapi_predecessor_change_list
resource: crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions.rs#L210-L242
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/predecessor_merge_is_idempotent_and_keeps_both_replicas
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/stale_advertised_fid_change_key_is_a_conflict_with_durable_server_version
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx
---

# Signature

`fn parse_mapi_predecessor_change_list(bytes: &[u8]) -> Result<MapiPredecessors>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [commit_mapi_associated_config_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx.md)
- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)
- [predecessor_merge_is_idempotent_and_keeps_both_replicas](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/predecessor_merge_is_idempotent_and_keeps_both_replicas.md)
- [stale_advertised_fid_change_key_is_a_conflict_with_durable_server_version](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/stale_advertised_fid_change_key_is_a_conflict_with_durable_server_version.md)
- [commit_mapi_navigation_shortcut_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx.md)