---
type: Rust Function
title: insert_external_blob_with_active_placement
resource: crates/lpe-storage/src/storage_visibility/tests.rs#L115-L169
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes
---

# Signature

`async fn insert_external_blob_with_active_placement( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, )`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes.md)