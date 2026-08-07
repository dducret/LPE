---
type: Rust Function
title: account_auth_factors
resource: crates/lpe-admin-api/src/client_auth.rs#L154-L164
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/auth/Storage/fetch_account_auth_factors
---

# Signature

`pub(crate) async fn account_auth_factors( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<AccountAuthFactorsResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [fetch_account_auth_factors](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_auth_factors.md)