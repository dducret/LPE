---
type: Rust Function
title: update_public_folder
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L75-L102
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn update_public_folder( State(storage): State<Storage>, headers: HeaderMap, AxumPath(folder_id): AxumPath<Uuid>, Json(request): Json<UpdatePublicFolderRequest>, ) -> ApiResult<PublicFolder>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)