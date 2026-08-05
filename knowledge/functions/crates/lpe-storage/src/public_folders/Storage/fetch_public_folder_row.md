---
type: Rust Method
title: fetch_public_folder_row
resource: crates/lpe-storage/src/public_folders.rs#L1322-L1338
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/public_folders/types/public_folder_select_sql
  - functions/crates/lpe-storage/src/public_folders/types/map_public_folder
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder
---

# Signature

`async fn fetch_public_folder_row( &self, account_id: Uuid, folder_id: Uuid, ) -> Result<PublicFolder>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [public_folder_select_sql](../../../../../../functions/crates/lpe-storage/src/public_folders/types/public_folder_select_sql.md)
- [map_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder.md)

# Called by

- [create_public_folder_child](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child.md)
- [fetch_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder.md)
- [update_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder.md)
- [delete_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder.md)