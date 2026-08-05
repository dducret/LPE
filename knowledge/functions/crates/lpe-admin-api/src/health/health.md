---
type: Rust Function
title: health
resource: crates/lpe-admin-api/src/health.rs#L12-L18
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
---

# Signature

`pub(crate) async fn health(State(storage): State<Storage>) -> ApiResult<HealthResponse>`

# Calls

- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)