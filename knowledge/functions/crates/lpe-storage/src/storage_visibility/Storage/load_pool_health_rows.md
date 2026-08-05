---
type: Rust Method
title: load_pool_health_rows
resource: crates/lpe-storage/src/storage_visibility.rs#L283-L337
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health
---

# Signature

`async fn load_pool_health_rows( &self, tenant_filter: Option<Uuid>, ) -> Result<Vec<PoolHealthRow>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [fetch_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_storage_health.md)