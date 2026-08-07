---
type: Rust Function
title: get_client_contact
resource: crates/lpe-admin-api/src/workspace.rs#L673-L687
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`pub(crate) async fn get_client_contact( State(storage): State<Storage>, headers: HeaderMap, AxumPath(contact_id): AxumPath<Uuid>, ) -> ApiResult<ClientContact>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)