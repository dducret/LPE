---
type: Rust Function
title: storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes
resource: crates/lpe-storage/src/storage_visibility/tests.rs#L497-L512
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/tests/insert_external_blob_with_active_placement
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_metadata_diagnostics
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes()`

# Calls

- [insert_external_blob_with_active_placement](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/insert_external_blob_with_active_placement.md)
- [fetch_storage_metadata_diagnostics](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_metadata_diagnostics.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)