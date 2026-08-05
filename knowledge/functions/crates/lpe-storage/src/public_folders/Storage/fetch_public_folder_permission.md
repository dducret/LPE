---
type: Rust Method
title: fetch_public_folder_permission
resource: crates/lpe-storage/src/public_folders.rs#L1340-L1378
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/map_public_folder_permission
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission
---

# Signature

`async fn fetch_public_folder_permission( &self, account_id: Uuid, folder_id: Uuid, principal_account_id: Uuid, ) -> Result<PublicFolderPermission>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [map_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder_permission.md)

# Called by

- [upsert_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission.md)