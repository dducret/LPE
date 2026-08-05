---
type: Rust Function
title: oidc_start
resource: crates/lpe-admin-api/src/admin_auth.rs#L298-L312
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-admin-api/src/http/public_origin
---

# Signature

`pub(crate) async fn oidc_start( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<OidcStartResponse>`

# Calls

- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [public_origin](../../../../../functions/crates/lpe-admin-api/src/http/public_origin.md)