---
type: Rust Method
title: fetch_public_folder_permissions
resource: crates/lpe-storage/src/public_folders.rs#L810-L844
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_share
---

# Signature

`pub async fn fetch_public_folder_permissions( &self, account_id: Uuid, folder_id: Uuid, ) -> Result<Vec<PublicFolderPermission>>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_share](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_share.md)