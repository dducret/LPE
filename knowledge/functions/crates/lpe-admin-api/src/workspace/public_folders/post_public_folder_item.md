---
type: Rust Function
title: post_public_folder_item
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L186-L206
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
  - functions/crates/lpe-admin-api/src/workspace/public_folders/map_public_folder_item_request
---

# Signature

`pub(crate) async fn post_public_folder_item( State(storage): State<Storage>, headers: HeaderMap, AxumPath(folder_id): AxumPath<Uuid>, Json(request): Json<UpsertPublicFolderItemRequest>, ) -> ApiResult<PublicFolderItem>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)
- [map_public_folder_item_request](../../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/map_public_folder_item_request.md)