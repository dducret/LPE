---
type: Rust Function
title: create_alias
resource: crates/lpe-admin-api/src/console.rs#L416-L441
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email
---

# Signature

`pub(crate) async fn create_alias( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateAliasRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [ensure_admin_can_manage_email](../../../../../functions/crates/lpe-admin-api/src/util/ensure_admin_can_manage_email.md)