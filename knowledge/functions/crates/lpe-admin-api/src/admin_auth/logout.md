---
type: Rust Function
title: logout
resource: crates/lpe-admin-api/src/admin_auth.rs#L123-L149
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session
  - functions/crates/lpe-storage/src/auth/Storage/delete_admin_session
---

# Signature

`pub(crate) async fn logout( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<HealthResponse>`

# Calls

- [fetch_admin_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/fetch_admin_session.md)
- [delete_admin_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/delete_admin_session.md)