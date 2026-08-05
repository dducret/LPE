---
type: Rust Function
title: mapi_xid_global_counter
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L169-L176
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/preflight_unknown_mapi_navigation_shortcut_delete_in_tx
---

# Signature

`fn mapi_xid_global_counter(xid: &[u8]) -> Result<u64>`

# Called by

- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)
- [preflight_unknown_mapi_navigation_shortcut_delete_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/navigation_shortcut_import/preflight_unknown_mapi_navigation_shortcut_delete_in_tx.md)