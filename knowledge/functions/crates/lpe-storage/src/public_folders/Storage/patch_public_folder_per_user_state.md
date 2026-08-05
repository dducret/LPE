---
type: Rust Method
title: patch_public_folder_per_user_state
resource: crates/lpe-storage/src/public_folders.rs#L1197-L1275
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_read
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_private_change
---

# Signature

`pub async fn patch_public_folder_per_user_state( &self, account_id: Uuid, folder_id: Uuid, patches: &[PublicFolderPerUserStatePatch], ) -> Result<Vec<PublicFolderPerUserState>>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_read](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_read.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [record_public_folder_private_change](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_private_change.md)