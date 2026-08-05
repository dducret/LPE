---
type: Rust Method
title: fetch_public_folder_per_user_state
resource: crates/lpe-storage/src/public_folders.rs#L1157-L1195
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_read
---

# Signature

`pub async fn fetch_public_folder_per_user_state( &self, account_id: Uuid, folder_id: Uuid, ) -> Result<Vec<PublicFolderPerUserState>>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_read](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_read.md)