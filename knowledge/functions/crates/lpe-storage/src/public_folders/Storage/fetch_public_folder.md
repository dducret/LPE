---
type: Rust Method
title: fetch_public_folder
resource: crates/lpe-storage/src/public_folders.rs#L223-L231
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_read
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_row
---

# Signature

`pub async fn fetch_public_folder( &self, account_id: Uuid, folder_id: Uuid, ) -> Result<PublicFolder>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_read](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_read.md)
- [fetch_public_folder_row](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_row.md)