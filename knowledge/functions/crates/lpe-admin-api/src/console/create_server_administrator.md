---
type: Rust Function
title: create_server_administrator
resource: crates/lpe-admin-api/src/console.rs#L658-L704
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-storage/src/auth/Storage/upsert_admin_credential
---

# Signature

`pub(crate) async fn create_server_administrator( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateServerAdministratorRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [upsert_admin_credential](../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_admin_credential.md)