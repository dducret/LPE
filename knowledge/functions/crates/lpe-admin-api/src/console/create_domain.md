---
type: Rust Function
title: create_domain
resource: crates/lpe-admin-api/src/console.rs#L359-L385
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
---

# Signature

`pub(crate) async fn create_domain( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateDomainRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)