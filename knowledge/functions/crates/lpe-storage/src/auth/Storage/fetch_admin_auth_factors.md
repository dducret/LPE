---
type: Rust Method
title: fetch_admin_auth_factors
resource: crates/lpe-storage/src/auth.rs#L276-L314
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/admin_auth_factors
  - functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor
  - functions/crates/lpe-admin-api/src/admin_auth/revoke_admin_factor
---

# Signature

`pub async fn fetch_admin_auth_factors( &self, admin_email: &str, ) -> Result<Vec<AdminAuthFactor>>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)

# Called by

- [admin_auth_factors](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/admin_auth_factors.md)
- [verify_totp_factor](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor.md)
- [revoke_admin_factor](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/revoke_admin_factor.md)