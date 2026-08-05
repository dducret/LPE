---
type: Rust Method
title: fetch_admin_login
resource: crates/lpe-storage/src/auth.rs#L909-L961
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  - functions/crates/lpe-storage/src/util/permissions_from_storage
  - functions/crates/lpe-storage/src/util/permission_summary
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/login
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_callback
---

# Signature

`pub async fn fetch_admin_login(&self, email: &str) -> Result<Option<AdminLogin>>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)
- [permissions_from_storage](../../../../../../functions/crates/lpe-storage/src/util/permissions_from_storage.md)
- [permission_summary](../../../../../../functions/crates/lpe-storage/src/util/permission_summary.md)

# Called by

- [login](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/login.md)
- [oidc_callback](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)