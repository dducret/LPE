---
type: Rust Function
title: patch_public_folder_item
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L208-L229
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/workspace/public_folders/map_public_folder_item_request
---

# Signature

`pub(crate) async fn patch_public_folder_item( State(storage): State<Storage>, headers: HeaderMap, AxumPath((folder_id, item_id)): AxumPath<(Uuid, Uuid)>, Json(mut request): Json<UpsertPublicFolderItemRequest>, ) -> ApiResult<PublicFolderItem>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [map_public_folder_item_request](../../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/map_public_folder_item_request.md)