---
type: Rust Method
title: fetch_public_folder_replicas
resource: crates/lpe-storage/src/public_folders.rs#L992-L1021
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_read
---

# Signature

`pub async fn fetch_public_folder_replicas( &self, account_id: Uuid, folder_id: Uuid, ) -> Result<Vec<PublicFolderReplica>>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_read](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_read.md)