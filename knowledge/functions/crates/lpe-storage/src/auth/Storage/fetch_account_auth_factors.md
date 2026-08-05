---
type: Rust Method
title: fetch_account_auth_factors
resource: crates/lpe-storage/src/auth.rs#L513-L551
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/account_auth_factors
  - functions/crates/lpe-admin-api/src/client_auth/verify_account_totp_factor
  - functions/crates/lpe-admin-api/src/client_auth/revoke_account_factor
---

# Signature

`pub async fn fetch_account_auth_factors( &self, account_email: &str, ) -> Result<Vec<AccountAuthFactor>>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)

# Called by

- [account_auth_factors](../../../../../../functions/crates/lpe-admin-api/src/client_auth/account_auth_factors.md)
- [verify_account_totp_factor](../../../../../../functions/crates/lpe-admin-api/src/client_auth/verify_account_totp_factor.md)
- [revoke_account_factor](../../../../../../functions/crates/lpe-admin-api/src/client_auth/revoke_account_factor.md)