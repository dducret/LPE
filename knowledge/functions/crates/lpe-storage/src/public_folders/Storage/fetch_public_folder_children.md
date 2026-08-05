---
type: Rust Method
title: fetch_public_folder_children
resource: crates/lpe-storage/src/public_folders.rs#L233-L249
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access
  - functions/crates/lpe-storage/src/public_folders/types/ensure_read
  - functions/crates/lpe-storage/src/public_folders/types/public_folder_select_sql
---

# Signature

`pub async fn fetch_public_folder_children( &self, account_id: Uuid, folder_id: Uuid, ) -> Result<Vec<PublicFolder>>`

# Calls

- [public_folder_access](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [ensure_read](../../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_read.md)
- [public_folder_select_sql](../../../../../../functions/crates/lpe-storage/src/public_folders/types/public_folder_select_sql.md)