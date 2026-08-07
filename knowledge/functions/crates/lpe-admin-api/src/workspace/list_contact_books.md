---
type: Rust Function
title: list_contact_books
resource: crates/lpe-admin-api/src/workspace.rs#L645-L656
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn list_contact_books( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<CollaborationCollection>>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)