---
type: Rust Function
title: client_me
resource: crates/lpe-admin-api/src/client_auth.rs#L144-L149
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn client_me( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<AuthenticatedAccount>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)