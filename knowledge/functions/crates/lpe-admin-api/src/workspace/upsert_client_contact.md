---
type: Rust Function
title: upsert_client_contact
resource: crates/lpe-admin-api/src/workspace.rs#L603-L624
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/workspace/contact_input_from_request
  - functions/crates/lpe-admin-api/src/workspace/client_contact_from_accessible
---

# Signature

`pub(crate) async fn upsert_client_contact( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<UpsertClientContactRequest>, ) -> ApiResult<ClientContact>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [contact_input_from_request](../../../../../functions/crates/lpe-admin-api/src/workspace/contact_input_from_request.md)
- [client_contact_from_accessible](../../../../../functions/crates/lpe-admin-api/src/workspace/client_contact_from_accessible.md)