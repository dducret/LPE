---
type: Rust Function
title: search_email_trace
resource: crates/lpe-admin-api/src/console.rs#L719-L733
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
---

# Signature

`pub(crate) async fn search_email_trace( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<EmailTraceSearchRequest>, ) -> ApiResult<Vec<EmailTraceResult>>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)