---
type: Rust Function
title: create_filter_rule
resource: crates/lpe-admin-api/src/console.rs#L706-L717
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
---

# Signature

`pub(crate) async fn create_filter_rule( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateFilterRuleRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)