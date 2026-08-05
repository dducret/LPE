---
type: Rust Function
title: mapi_predecessors_contain_change_key
resource: crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions.rs#L244-L259
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx
---

# Signature

`fn mapi_predecessors_contain_change_key( entries: &MapiPredecessors, change_key: &[u8], ) -> Result<bool>`

# Calls

- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [commit_mapi_associated_config_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/associated_config_import/commit_mapi_associated_config_update_in_tx.md)
- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)
- [commit_mapi_navigation_shortcut_update_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_update/commit_mapi_navigation_shortcut_update_in_tx.md)