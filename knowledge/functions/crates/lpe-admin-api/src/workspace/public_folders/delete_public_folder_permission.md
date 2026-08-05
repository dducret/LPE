---
type: Rust Function
title: delete_public_folder_permission
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L300-L323
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn delete_public_folder_permission( State(storage): State<Storage>, headers: HeaderMap, AxumPath((folder_id, principal_id)): AxumPath<(Uuid, Uuid)>, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)