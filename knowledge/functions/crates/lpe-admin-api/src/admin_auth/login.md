---
type: Rust Function
title: login
resource: crates/lpe-admin-api/src/admin_auth.rs#L25-L121
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_totp_secret
  - functions/crates/lpe-admin-api/src/totp/verify_code
  - functions/crates/lpe-storage/src/auth/Storage/create_admin_session
  - functions/crates/lpe-admin-api/src/security/admin_session_minutes
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session
---

# Signature

`pub(crate) async fn login( State(storage): State<Storage>, Json(request): Json<LoginRequest>, ) -> ApiResult<LoginResponse>`

# Calls

- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [fetch_admin_login](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login.md)
- [fetch_admin_totp_secret](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_totp_secret.md)
- [verify_code](../../../../../functions/crates/lpe-admin-api/src/totp/verify_code.md)
- [create_admin_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/create_admin_session.md)
- [admin_session_minutes](../../../../../functions/crates/lpe-admin-api/src/security/admin_session_minutes.md)
- [fetch_admin_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session.md)