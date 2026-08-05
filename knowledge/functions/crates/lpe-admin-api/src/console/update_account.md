---
type: Rust Function
title: update_account
resource: crates/lpe-admin-api/src/console.rs#L154-L208
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email
---

# Signature

`pub(crate) async fn update_account( State(storage): State<Storage>, headers: HeaderMap, AxumPath(account_id): AxumPath<Uuid>, Json(request): Json<UpdateAccountRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [ensure_admin_can_manage_email](../../../../../functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email.md)