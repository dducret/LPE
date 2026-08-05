---
type: Rust Function
title: client_logout
resource: crates/lpe-admin-api/src/client_auth.rs#L128-L142
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/auth/Storage/delete_account_session
---

# Signature

`pub(crate) async fn client_logout( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<HealthResponse>`

# Calls

- [delete_account_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/delete_account_session.md)