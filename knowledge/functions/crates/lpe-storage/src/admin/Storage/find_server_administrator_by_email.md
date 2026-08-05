---
type: Rust Method
title: find_server_administrator_by_email
resource: crates/lpe-storage/src/admin.rs#L71-L118
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  - functions/crates/lpe-storage/src/util/permissions_from_storage
  - functions/crates/lpe-storage/src/util/permission_summary
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_callback
---

# Signature

`pub async fn find_server_administrator_by_email( &self, email: &str, ) -> Result<Option<ServerAdministrator>>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)
- [permissions_from_storage](../../../../../../functions/crates/lpe-storage/src/util/permissions_from_storage.md)
- [permission_summary](../../../../../../functions/crates/lpe-storage/src/util/permission_summary.md)

# Called by

- [oidc_callback](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)