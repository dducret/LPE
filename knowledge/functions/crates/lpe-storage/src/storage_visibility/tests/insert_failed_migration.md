---
type: Rust Function
title: insert_failed_migration
resource: crates/lpe-storage/src/storage_visibility/tests.rs#L171-L204
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/tests/storage_health_reports_degraded_and_tenant_scoped_counts
---

# Signature

`async fn insert_failed_migration( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, blob_id: Uuid, source_placement_id: Uuid, )`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [storage_health_reports_degraded_and_tenant_scoped_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_health_reports_degraded_and_tenant_scoped_counts.md)