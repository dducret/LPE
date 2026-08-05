---
type: Rust Function
title: upload_pst_import
resource: crates/lpe-admin-api/src/console.rs#L276-L357
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-admin-api/src/util/mailbox_account_email
  - functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email
  - functions/tools/rca_outlook/http/content_type
  - functions/crates/lpe-admin-api/src/pst/pst_import_dir
  - functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file
---

# Signature

`pub(crate) async fn upload_pst_import( State(storage): State<Storage>, headers: HeaderMap, AxumPath(mailbox_id): AxumPath<Uuid>, mut multipart: Multipart, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [mailbox_account_email](../../../../../functions/crates/lpe-admin-api/src/util/mailbox_account_email.md)
- [ensure_admin_can_manage_email](../../../../../functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email.md)
- [content_type](../../../../../functions/tools/rca_outlook/http/content_type.md)
- [pst_import_dir](../../../../../functions/crates/lpe-admin-api/src/pst/pst_import_dir.md)
- [validate_uploaded_pst_file](../../../../../functions/crates/lpe-admin-api/src/pst/validate_uploaded_pst_file.md)