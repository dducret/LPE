---
type: Rust Function
title: ensure_read
resource: crates/lpe-storage/src/public_folders/types.rs#L184-L190
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_children
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_replicas
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_per_user_state
  - functions/crates/lpe-storage/src/public_folders/Storage/patch_public_folder_per_user_state
---

# Signature

`pub(crate) fn ensure_read(access: PublicFolderAccess) -> Result<()>`

# Called by

- [fetch_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder.md)
- [fetch_public_folder_children](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_children.md)
- [fetch_public_folder_items](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items.md)
- [fetch_public_folder_replicas](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_replicas.md)
- [fetch_public_folder_per_user_state](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_per_user_state.md)
- [patch_public_folder_per_user_state](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/patch_public_folder_per_user_state.md)