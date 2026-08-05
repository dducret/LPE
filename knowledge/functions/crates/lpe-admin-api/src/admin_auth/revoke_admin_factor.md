---
type: Rust Function
title: revoke_admin_factor
resource: crates/lpe-admin-api/src/admin_auth.rs#L254-L282
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/auth/Storage/revoke_admin_auth_factor
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_auth_factors
---

# Signature

`pub(crate) async fn revoke_admin_factor( State(storage): State<Storage>, headers: HeaderMap, AxumPath(factor_id): AxumPath<Uuid>, ) -> ApiResult<AdminAuthFactorsResponse>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [revoke_admin_auth_factor](../../../../../functions/crates/lpe-storage/src/auth/Storage/revoke_admin_auth_factor.md)
- [fetch_admin_auth_factors](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_auth_factors.md)