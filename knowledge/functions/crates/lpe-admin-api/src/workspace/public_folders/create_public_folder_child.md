---
type: Rust Function
title: create_public_folder_child
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L142-L170
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn create_public_folder_child( State(storage): State<Storage>, headers: HeaderMap, AxumPath(folder_id): AxumPath<Uuid>, Json(request): Json<CreatePublicFolderRequest>, ) -> ApiResult<PublicFolder>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)