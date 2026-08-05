---
type: Rust Function
title: list_public_folder_trees
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L24-L35
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn list_public_folder_trees( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<PublicFolderTree>>`

# Calls

- [require_account](../../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)