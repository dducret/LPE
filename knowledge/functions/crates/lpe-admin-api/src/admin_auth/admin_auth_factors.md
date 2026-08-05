---
type: Rust Function
title: admin_auth_factors
resource: crates/lpe-admin-api/src/admin_auth.rs#L158-L168
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_auth_factors
---

# Signature

`pub(crate) async fn admin_auth_factors( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<AdminAuthFactorsResponse>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [fetch_admin_auth_factors](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_auth_factors.md)