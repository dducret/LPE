---
type: Rust Function
title: client_oidc_metadata
resource: crates/lpe-admin-api/src/client_auth.rs#L408-L420
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
---

# Signature

`pub(crate) async fn client_oidc_metadata( State(storage): State<Storage>, ) -> ApiResult<ClientOidcMetadataResponse>`

# Calls

- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)