---
type: Rust Function
title: create_pst_transfer_job
resource: crates/lpe-admin-api/src/console.rs#L236-L274
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-admin-api/src/util/mailbox_account_email
  - functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email
---

# Signature

`pub(crate) async fn create_pst_transfer_job( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreatePstTransferJobRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [mailbox_account_email](../../../../../functions/crates/lpe-admin-api/src/util/mailbox_account_email.md)
- [ensure_admin_can_manage_email](../../../../../functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email.md)