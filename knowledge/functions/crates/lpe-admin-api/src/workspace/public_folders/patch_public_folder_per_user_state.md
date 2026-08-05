---
type: Rust Function
title: patch_public_folder_per_user_state
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L405-L428
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn patch_public_folder_per_user_state( State(storage): State<Storage>, headers: HeaderMap, AxumPath(folder_id): AxumPath<Uuid>, Json(request): Json<PublicFolderPerUserStatePatchBatchRequest>, ) -> ApiResult<Vec<PublicFolderPerUserState>>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)