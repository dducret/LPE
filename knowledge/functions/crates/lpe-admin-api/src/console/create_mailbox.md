---
type: Rust Function
title: create_mailbox
resource: crates/lpe-admin-api/src/console.rs#L210-L234
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
---

# Signature

`pub(crate) async fn create_mailbox( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateMailboxRequest>, ) -> ApiResult<AdminDashboard>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)