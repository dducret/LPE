---
type: Rust Method
title: public_folder_access
resource: crates/lpe-storage/src/public_folders.rs#L1277-L1320
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_children
  - functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_item
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_item
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permissions
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_permission
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_replicas
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_replica
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_per_user_state
  - functions/crates/lpe-storage/src/public_folders/Storage/patch_public_folder_per_user_state
  - functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permission
---

# Signature

`async fn public_folder_access( &self, account_id: Uuid, folder_id: Uuid, ) -> Result<PublicFolderAccess>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)

# Called by

- [create_public_folder_child](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child.md)
- [fetch_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder.md)
- [fetch_public_folder_children](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_children.md)
- [update_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder.md)
- [delete_public_folder](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder.md)
- [fetch_public_folder_items](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items.md)
- [upsert_public_folder_item](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_item.md)
- [delete_public_folder_item](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_item.md)
- [fetch_public_folder_permissions](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permissions.md)
- [upsert_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission.md)
- [delete_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_permission.md)
- [fetch_public_folder_replicas](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_replicas.md)
- [upsert_public_folder_replica](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica.md)
- [delete_public_folder_replica](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_replica.md)
- [fetch_public_folder_per_user_state](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_per_user_state.md)
- [patch_public_folder_per_user_state](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/patch_public_folder_per_user_state.md)
- [fetch_public_folder_permission](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permission.md)