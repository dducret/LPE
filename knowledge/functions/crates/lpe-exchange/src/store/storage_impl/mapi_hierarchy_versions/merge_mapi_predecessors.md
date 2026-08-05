---
type: Rust Function
title: merge_mapi_predecessors
resource: crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions.rs#L279-L299
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessor_change_key
---

# Signature

`fn merge_mapi_predecessors( current: &mut MapiPredecessors, incoming: MapiPredecessors, ) -> Result<()>`

# Called by

- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)
- [merge_mapi_predecessor_change_key](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions/merge_mapi_predecessor_change_key.md)