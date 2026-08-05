---
type: Rust Function
title: oidc_metadata
resource: crates/lpe-admin-api/src/admin_auth.rs#L284-L296
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
---

# Signature

`pub(crate) async fn oidc_metadata( State(storage): State<Storage>, ) -> ApiResult<OidcMetadataResponse>`

# Calls

- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)