---
type: Rust Function
title: revoke_account_factor
resource: crates/lpe-admin-api/src/client_auth.rs#L254-L282
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/auth/Storage/revoke_account_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/fetch_account_auth_factors
---

# Signature

`pub(crate) async fn revoke_account_factor( State(storage): State<Storage>, headers: HeaderMap, AxumPath(factor_id): AxumPath<Uuid>, ) -> ApiResult<AccountAuthFactorsResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [revoke_account_auth_factor](../../../../../functions/crates/lpe-storage/src/auth/Storage/revoke_account_auth_factor.md)
- [fetch_account_auth_factors](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_account_auth_factors.md)