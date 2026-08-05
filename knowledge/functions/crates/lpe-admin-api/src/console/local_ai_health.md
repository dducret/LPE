---
type: Rust Function
title: local_ai_health
resource: crates/lpe-admin-api/src/console.rs#L31-L46
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
---

# Signature

`pub(crate) async fn local_ai_health( State(storage): State<Storage>, ) -> ApiResult<LocalAiHealthResponse>`

# Calls

- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)