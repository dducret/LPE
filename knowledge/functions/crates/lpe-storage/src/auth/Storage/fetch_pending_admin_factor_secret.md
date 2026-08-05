---
type: Rust Method
title: fetch_pending_admin_factor_secret
resource: crates/lpe-storage/src/auth.rs#L351-L375
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor
---

# Signature

`pub async fn fetch_pending_admin_factor_secret( &self, admin_email: &str, factor_id: Uuid, ) -> Result<Option<String>>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)

# Called by

- [verify_totp_factor](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor.md)