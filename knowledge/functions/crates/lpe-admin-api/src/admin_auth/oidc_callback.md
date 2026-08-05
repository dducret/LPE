---
type: Rust Function
title: oidc_callback
resource: crates/lpe-admin-api/src/admin_auth.rs#L314-L399
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-admin-api/src/http/public_origin
  - functions/crates/lpe-storage/src/auth/Storage/find_admin_oidc_identity
  - functions/crates/lpe-storage/src/admin/Storage/find_server_administrator_by_email
  - functions/crates/lpe-storage/src/auth/Storage/upsert_admin_oidc_identity
  - functions/crates/lpe-storage/src/auth/Storage/ensure_admin_credential_stub
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login
  - functions/crates/lpe-storage/src/auth/Storage/create_admin_session
  - functions/crates/lpe-admin-api/src/security/admin_session_minutes
---

# Signature

`pub(crate) async fn oidc_callback( State(storage): State<Storage>, headers: HeaderMap, Query(params): Query<HashMap<String, String>>, ) -> Result<Redirect, (StatusCode, String)>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [public_origin](../../../../../functions/crates/lpe-admin-api/src/http/public_origin.md)
- [find_admin_oidc_identity](../../../../../functions/crates/lpe-storage/src/auth/Storage/find_admin_oidc_identity.md)
- [find_server_administrator_by_email](../../../../../functions/crates/lpe-storage/src/admin/Storage/find_server_administrator_by_email.md)
- [upsert_admin_oidc_identity](../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_admin_oidc_identity.md)
- [ensure_admin_credential_stub](../../../../../functions/crates/lpe-storage/src/auth/Storage/ensure_admin_credential_stub.md)
- [fetch_admin_login](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_login.md)
- [create_admin_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/create_admin_session.md)
- [admin_session_minutes](../../../../../functions/crates/lpe-admin-api/src/security/admin_session_minutes.md)