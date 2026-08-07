---
type: Rust Function
title: list_account_app_passwords
resource: crates/lpe-admin-api/src/client_auth.rs#L284-L294
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn list_account_app_passwords( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<AccountAppPasswordsResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)