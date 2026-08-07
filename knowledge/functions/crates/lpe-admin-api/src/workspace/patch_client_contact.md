---
type: Rust Function
title: patch_client_contact
resource: crates/lpe-admin-api/src/workspace.rs#L689-L737
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-admin-api/src/types/PatchField/is_present
---

# Signature

`pub(crate) async fn patch_client_contact( State(storage): State<Storage>, headers: HeaderMap, AxumPath(contact_id): AxumPath<Uuid>, Json(request): Json<PatchClientContactRequest>, ) -> ApiResult<ClientContact>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [is_present](../../../../../functions/crates/lpe-admin-api/src/types/PatchField/is_present.md)