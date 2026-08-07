---
type: Rust Function
title: verify_account_totp_factor
resource: crates/lpe-admin-api/src/client_auth.rs#L216-L252
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/auth/Storage/fetch_pending_account_factor_secret
  - functions/crates/lpe-admin-api/src/totp/verify_code
  - functions/crates/lpe-storage/src/auth/Storage/activate_account_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/fetch_account_auth_factors
---

# Signature

`pub(crate) async fn verify_account_totp_factor( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<VerifyTotpRequest>, ) -> ApiResult<AccountAuthFactorsResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [fetch_pending_account_factor_secret](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_pending_account_factor_secret.md)
- [verify_code](../../../../../functions/crates/lpe-admin-api/src/totp/verify_code.md)
- [activate_account_auth_factor](../../../../../functions/crates/lpe-storage/src/auth/Storage/activate_account_auth_factor.md)
- [fetch_account_auth_factors](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_auth_factors.md)