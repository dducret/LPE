---
type: Rust Method
title: fetch_public_folder_items_by_ids
resource: crates/lpe-storage/src/public_folders.rs#L594-L632
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/public_folders/types/public_folder_item_select_sql
---

# Signature

`pub async fn fetch_public_folder_items_by_ids( &self, account_id: Uuid, item_ids: &[Uuid], ) -> Result<Vec<PublicFolderItem>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [public_folder_item_select_sql](../../../../../../functions/crates/lpe-storage/src/public_folders/types/public_folder_item_select_sql.md)