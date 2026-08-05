---
type: Rust Function
title: create_account
resource: crates/lpe-admin-api/src/console.rs#L67-L152
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-storage/src/auth/Storage/upsert_account_credential
---

# Signature

`pub(crate) async fn create_account( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateAccountRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [ensure_admin_can_manage_email](../../../../../functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [upsert_account_credential](../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_account_credential.md)