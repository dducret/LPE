---
type: Rust Method
title: fetch_admin_totp_secret
resource: crates/lpe-storage/src/auth.rs#L316-L349
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/login
---

# Signature

`pub async fn fetch_admin_totp_secret( &self, admin_email: &str, ) -> Result<Option<(Uuid, String)>>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)

# Called by

- [login](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/login.md)