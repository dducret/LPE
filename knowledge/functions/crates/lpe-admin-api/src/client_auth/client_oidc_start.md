---
type: Rust Function
title: client_oidc_start
resource: crates/lpe-admin-api/src/client_auth.rs#L422-L436
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  - functions/crates/lpe-admin-api/src/http/public_origin
---

# Signature

`pub(crate) async fn client_oidc_start( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<ClientOidcStartResponse>`

# Calls

- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)
- [public_origin](../../../../../functions/crates/lpe-admin-api/src/http/public_origin.md)