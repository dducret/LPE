---
type: Rust Function
title: me
resource: crates/lpe-admin-api/src/admin_auth.rs#L151-L156
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
---

# Signature

`pub(crate) async fn me( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<AuthenticatedAdmin>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)