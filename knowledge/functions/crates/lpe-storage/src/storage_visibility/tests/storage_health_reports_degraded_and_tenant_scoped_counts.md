---
type: Rust Function
title: storage_health_reports_degraded_and_tenant_scoped_counts
resource: crates/lpe-storage/src/storage_visibility/tests.rs#L335-L361
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_visibility/tests/insert_placement
  - functions/crates/lpe-storage/src/storage_visibility/tests/insert_failed_migration
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_health
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_health
---

# Signature

`async fn storage_health_reports_degraded_and_tenant_scoped_counts()`

# Calls

- [insert_placement](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/insert_placement.md)
- [insert_failed_migration](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/insert_failed_migration.md)
- [fetch_platform_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_health.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [fetch_tenant_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_tenant_storage_health.md)