---
type: Rust Function
title: enroll_account_totp
resource: crates/lpe-admin-api/src/client_auth.rs#L166-L210
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-admin-api/src/totp/generate_secret
  - functions/crates/lpe-storage/src/auth/Storage/create_account_auth_factor
  - functions/crates/lpe-admin-api/src/totp/otpauth_url
---

# Signature

`pub(crate) async fn enroll_account_totp( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<EnrollTotpRequest>, ) -> ApiResult<EnrollTotpResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [generate_secret](../../../../../functions/crates/lpe-admin-api/src/totp/generate_secret.md)
- [create_account_auth_factor](../../../../../functions/crates/lpe-storage/src/auth/Storage/create_account_auth_factor.md)
- [otpauth_url](../../../../../functions/crates/lpe-admin-api/src/totp/otpauth_url.md)