---
type: Rust Function
title: client_login
resource: crates/lpe-admin-api/src/client_auth.rs#L31-L126
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-storage/src/auth/Storage/fetch_account_totp_secret
  - functions/crates/lpe-admin-api/src/totp/verify_code
  - functions/crates/lpe-storage/src/auth/Storage/create_account_session
  - functions/crates/lpe-admin-api/src/security/client_session_minutes
  - functions/crates/lpe-admin-api/src/client_auth/mail_session_headers
---

# Signature

`pub(crate) async fn client_login( State(storage): State<Storage>, Json(request): Json<LoginRequest>, ) -> Result<(HeaderMap, Json<ClientLoginResponse>), (StatusCode, String)>`

# Calls

- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [fetch_account_totp_secret](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_totp_secret.md)
- [verify_code](../../../../../functions/crates/lpe-admin-api/src/totp/verify_code.md)
- [create_account_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/create_account_session.md)
- [client_session_minutes](../../../../../functions/crates/lpe-admin-api/src/security/client_session_minutes.md)
- [mail_session_headers](../../../../../functions/crates/lpe-admin-api/src/client_auth/mail_session_headers.md)