---
type: Rust Function
title: list_client_contacts
resource: crates/lpe-admin-api/src/workspace.rs#L658-L671
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn list_client_contacts( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<ClientContact>>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)