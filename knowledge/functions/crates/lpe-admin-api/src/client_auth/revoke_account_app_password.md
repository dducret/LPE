---
type: Rust Function
title: revoke_account_app_password
resource: crates/lpe-admin-api/src/client_auth.rs#L332-L360
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn revoke_account_app_password( State(storage): State<Storage>, headers: HeaderMap, AxumPath(app_password_id): AxumPath<Uuid>, ) -> ApiResult<AccountAppPasswordsResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)