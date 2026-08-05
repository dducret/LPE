---
type: Rust Function
title: list_public_folder_permissions
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L256-L268
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn list_public_folder_permissions( State(storage): State<Storage>, headers: HeaderMap, AxumPath(folder_id): AxumPath<Uuid>, ) -> ApiResult<Vec<PublicFolderPermission>>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)