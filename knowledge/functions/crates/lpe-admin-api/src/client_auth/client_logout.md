---
type: Rust Function
title: client_logout
resource: crates/lpe-admin-api/src/client_auth.rs#L128-L145
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/http/account_session_token
  - functions/crates/lpe-storage/src/auth/Storage/delete_account_session
  - functions/crates/lpe-admin-api/src/client_auth/cleared_mail_session_headers
---

# Signature

`pub(crate) async fn client_logout( State(storage): State<Storage>, headers: HeaderMap, ) -> Result<(HeaderMap, Json<HealthResponse>), (StatusCode, String)>`

# Calls

- [account_session_token](../../../../../functions/crates/lpe-admin-api/src/http/account_session_token.md)
- [delete_account_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/delete_account_session.md)
- [cleared_mail_session_headers](../../../../../functions/crates/lpe-admin-api/src/client_auth/cleared_mail_session_headers.md)