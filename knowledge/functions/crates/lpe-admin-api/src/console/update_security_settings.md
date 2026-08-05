---
type: Rust Function
title: update_security_settings
resource: crates/lpe-admin-api/src/console.rs#L482-L602
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-storage/src/admin/Storage/update_settings
---

# Signature

`pub(crate) async fn update_security_settings( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpdateSecuritySettingsRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [update_settings](../../../../../functions/crates/lpe-storage/src/admin/Storage/update_settings.md)