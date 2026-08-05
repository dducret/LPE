---
type: Rust Function
title: ensure_tree_admin
resource: crates/lpe-storage/src/public_folders/types.rs#L216-L222
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child
  - functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder
---

# Signature

`pub(crate) fn ensure_tree_admin(account_id: Uuid, access: PublicFolderAccess) -> Result<()>`

# Called by

- [create_public_folder_child](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child.md)
- [update_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder.md)
- [delete_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder.md)