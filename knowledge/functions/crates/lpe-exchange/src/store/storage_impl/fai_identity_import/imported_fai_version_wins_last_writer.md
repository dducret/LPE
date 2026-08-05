---
type: Rust Function
title: imported_fai_version_wins_last_writer
resource: crates/lpe-exchange/src/store/storage_impl/fai_identity_import.rs#L8-L28
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx
---

# Signature

`fn imported_fai_version_wins_last_writer( incoming_last_modification_time: u64, incoming_change_key: &[u8], current_last_modification_time: u64, current_change_key: &[u8], ) -> Result<bool>`

# Called by

- [commit_mapi_imported_fai_identity_in_tx](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/fai_identity_import/commit_mapi_imported_fai_identity_in_tx.md)