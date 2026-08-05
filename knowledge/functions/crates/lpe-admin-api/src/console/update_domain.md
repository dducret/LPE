---
type: Rust Function
title: update_domain
resource: crates/lpe-admin-api/src/console.rs#L387-L414
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
---

# Signature

`pub(crate) async fn update_domain( State(storage): State<Storage>, headers: HeaderMap, AxumPath(domain_id): AxumPath<Uuid>, Json(request): Json<UpdateDomainRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)