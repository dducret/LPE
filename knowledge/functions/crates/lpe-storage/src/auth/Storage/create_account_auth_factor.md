---
type: Rust Method
title: create_account_auth_factor
resource: crates/lpe-storage/src/auth.rs#L489-L511
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp
---

# Signature

`pub async fn create_account_auth_factor(&self, input: NewAccountAuthFactor) -> Result<Uuid>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [enroll_account_totp](../../../../../../functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp.md)