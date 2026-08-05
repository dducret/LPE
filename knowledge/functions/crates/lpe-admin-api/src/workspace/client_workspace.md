---
type: Rust Function
title: client_workspace
resource: crates/lpe-admin-api/src/workspace.rs#L295-L306
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-storage/src/workspace/Storage/fetch_client_workspace
---

# Signature

`pub(crate) async fn client_workspace( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<ClientWorkspace>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [fetch_client_workspace](../../../../../functions/crates/lpe-storage/src/workspace/Storage/fetch_client_workspace.md)