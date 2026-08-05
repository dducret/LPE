---
type: Rust Function
title: ensure_share
resource: crates/lpe-storage/src/public_folders/types.rs#L208-L214
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permissions
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_permission
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_replica
---

# Signature

`pub(crate) fn ensure_share(access: PublicFolderAccess) -> Result<()>`

# Called by

- [fetch_public_folder_permissions](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permissions.md)
- [upsert_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission.md)
- [delete_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_permission.md)
- [upsert_public_folder_replica](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica.md)
- [delete_public_folder_replica](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_replica.md)