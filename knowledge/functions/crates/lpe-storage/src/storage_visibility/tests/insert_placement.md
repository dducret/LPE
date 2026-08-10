---
type: Rust Function
title: insert_placement
resource: crates/lpe-storage/src/storage_visibility/tests.rs#L78-L113
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/tests/storage_health_reports_degraded_and_tenant_scoped_counts
  - functions/crates/lpe-storage/src/storage_visibility/tests/cleanup_visibility_reports_blockers_without_blob_or_placement_ids
---

# Signature

`async fn insert_placement( storage: &Storage, tenant_id: Uuid, domain_id: Uuid, blob_id: Uuid, status: &str, ) -> Uuid`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [storage_health_reports_degraded_and_tenant_scoped_counts](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/storage_health_reports_degraded_and_tenant_scoped_counts.md)
- [cleanup_visibility_reports_blockers_without_blob_or_placement_ids](../../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/cleanup_visibility_reports_blockers_without_blob_or_placement_ids.md)