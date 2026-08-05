---
type: Rust Function
title: verify_totp_factor
resource: crates/lpe-admin-api/src/admin_auth.rs#L216-L252
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/auth/Storage/fetch_pending_admin_factor_secret
  - functions/crates/lpe-admin-api/src/totp/verify_code
  - functions/crates/lpe-storage/src/auth/Storage/activate_admin_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_auth_factors
---

# Signature

`pub(crate) async fn verify_totp_factor( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<VerifyTotpRequest>, ) -> ApiResult<AdminAuthFactorsResponse>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [fetch_pending_admin_factor_secret](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_pending_admin_factor_secret.md)
- [verify_code](../../../../../functions/crates/lpe-admin-api/src/totp/verify_code.md)
- [activate_admin_auth_factor](../../../../../functions/crates/lpe-storage/src/auth/Storage/activate_admin_auth_factor.md)
- [fetch_admin_auth_factors](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_auth_factors.md)