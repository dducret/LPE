---
type: Rust Method
title: create_admin_auth_factor
resource: crates/lpe-storage/src/auth.rs#L252-L274
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/enroll_totp
---

# Signature

`pub async fn create_admin_auth_factor(&self, input: NewAdminAuthFactor) -> Result<Uuid>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [enroll_totp](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/enroll_totp.md)