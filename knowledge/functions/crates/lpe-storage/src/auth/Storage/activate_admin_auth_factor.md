---
type: Rust Method
title: activate_admin_auth_factor
resource: crates/lpe-storage/src/auth.rs#L377-L401
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor
---

# Signature

`pub async fn activate_admin_auth_factor( &self, admin_email: &str, factor_id: Uuid, ) -> Result<bool>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [verify_totp_factor](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor.md)