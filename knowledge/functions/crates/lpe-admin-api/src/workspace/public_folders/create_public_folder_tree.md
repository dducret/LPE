---
type: Rust Function
title: create_public_folder_tree
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L37-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn create_public_folder_tree( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreatePublicFolderTreeRequest>, ) -> ApiResult<PublicFolder>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)