---
type: Rust Function
title: cleanup_visibility_reports_blockers_without_blob_or_placement_ids
resource: crates/lpe-storage/src/storage_visibility/tests.rs#L460-L479
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/tests/insert_placement
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_cleanup
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn cleanup_visibility_reports_blockers_without_blob_or_placement_ids()`

# Calls

- [insert_placement](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/insert_placement.md)
- [fetch_tenant_storage_cleanup](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_cleanup.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)