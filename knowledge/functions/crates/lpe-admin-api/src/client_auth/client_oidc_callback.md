---
type: Rust Function
title: client_oidc_callback
resource: crates/lpe-admin-api/src/client_auth.rs#L434-L520
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-admin-api/src/http/public_origin
  - functions/crates/lpe-storage/src/auth/Storage/find_account_oidc_identity
  - functions/crates/lpe-storage/src/auth/Storage/upsert_account_oidc_identity
  - functions/crates/lpe-storage/src/auth/Storage/create_account_session
  - functions/crates/lpe-admin-api/src/security/client_session_minutes
  - functions/crates/lpe-admin-api/src/client_auth/mail_session_headers
---

# Signature

`pub(crate) async fn client_oidc_callback( State(storage): State<Storage>, headers: HeaderMap, Query(params): Query<HashMap<String, String>>, ) -> Result<(HeaderMap, Redirect), (StatusCode, String)>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [public_origin](../../../../../functions/crates/lpe-admin-api/src/http/public_origin.md)
- [find_account_oidc_identity](../../../../../functions/crates/lpe-storage/src/auth/Storage/find_account_oidc_identity.md)
- [upsert_account_oidc_identity](../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_account_oidc_identity.md)
- [create_account_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/create_account_session.md)
- [client_session_minutes](../../../../../functions/crates/lpe-admin-api/src/security/client_session_minutes.md)
- [mail_session_headers](../../../../../functions/crates/lpe-admin-api/src/client_auth/mail_session_headers.md)