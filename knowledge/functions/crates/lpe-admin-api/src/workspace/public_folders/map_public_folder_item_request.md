---
type: Rust Function
title: map_public_folder_item_request
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L430-L450
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/public_folders/post_public_folder_item
  - functions/crates/lpe-admin-api/src/workspace/public_folders/patch_public_folder_item
---

# Signature

`fn map_public_folder_item_request( account_id: Uuid, public_folder_id: Uuid, request: UpsertPublicFolderItemRequest, ) -> UpsertPublicFolderItemInput`

# Called by

- [post_public_folder_item](../../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/post_public_folder_item.md)
- [patch_public_folder_item](../../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/patch_public_folder_item.md)