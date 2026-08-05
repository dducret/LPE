---
type: Rust Function
title: mapi_predecessors_include
resource: crates/lpe-exchange/src/store/storage_impl/mapi_hierarchy_versions.rs#L261-L277
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
---

# Signature

`fn mapi_predecessors_include( current: &MapiPredecessors, incoming: &MapiPredecessors, ) -> Result<bool>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)