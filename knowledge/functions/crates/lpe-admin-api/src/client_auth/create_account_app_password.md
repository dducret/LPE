---
type: Rust Function
title: create_account_app_password
resource: crates/lpe-admin-api/src/client_auth.rs#L296-L330
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/security/generate_app_password_secret
---

# Signature

`pub(crate) async fn create_account_app_password( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateAccountAppPasswordRequest>, ) -> ApiResult<CreateAccountAppPasswordResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [generate_app_password_secret](../../../../../functions/crates/lpe-admin-api/src/security/generate_app_password_secret.md)