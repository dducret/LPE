---
type: Rust Method
title: record_public_folder_change
resource: crates/lpe-storage/src/public_folders/changes.rs#L9-L32
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_tree
  - functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child
  - functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_item
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_item
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica
  - functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_replica
---

# Signature

`pub(super) async fn record_public_folder_change( &self, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, access: &PublicFolderAccess, actor_account_id: Uuid, folder_id: Uuid, object_kind: &str, object_id: Uuid, change_kind: &str, summary_json: serde_json::Value, ) -> Result<i64>`

# Calls

- [record_public_folder_change_with_extra_affected](../../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected.md)

# Called by

- [create_public_folder_tree](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_tree.md)
- [create_public_folder_child](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child.md)
- [update_public_folder](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder.md)
- [delete_public_folder](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder.md)
- [upsert_public_folder_item](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_item.md)
- [delete_public_folder_item](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_item.md)
- [upsert_public_folder_replica](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica.md)
- [delete_public_folder_replica](../../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_replica.md)