---
type: Rust Module
title: types
resource: crates/lpe-storage/src/public_folders/types.rs#L1-L393
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-deserialize-serialize
  - external/uuid-uuid
  - external/crate-publicfolderitemrow-publicfolderperuserstaterow-publicfolderpermissionrow-publicfolderreplicarow-publicfolderrow-publicfoldertreerow
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [PublicFolderRights](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderRights.md)
- [PublicFolderTree](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderTree.md)
- [CreatePublicFolderTreeInput](../../../../../classes/crates/lpe-storage/src/public_folders/types/CreatePublicFolderTreeInput.md)
- [PublicFolder](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolder.md)
- [CreatePublicFolderInput](../../../../../classes/crates/lpe-storage/src/public_folders/types/CreatePublicFolderInput.md)
- [UpdatePublicFolderInput](../../../../../classes/crates/lpe-storage/src/public_folders/types/UpdatePublicFolderInput.md)
- [PublicFolderItem](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderItem.md)
- [UpsertPublicFolderItemInput](../../../../../classes/crates/lpe-storage/src/public_folders/types/UpsertPublicFolderItemInput.md)
- [PublicFolderPermission](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderPermission.md)
- [PublicFolderPermissionInput](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderPermissionInput.md)
- [PublicFolderReplica](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderReplica.md)
- [PublicFolderReplicaInput](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderReplicaInput.md)
- [PublicFolderPerUserState](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderPerUserState.md)
- [PublicFolderPerUserStatePatch](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderPerUserStatePatch.md)
- [PublicFolderAccess](../../../../../classes/crates/lpe-storage/src/public_folders/types/PublicFolderAccess.md)
- [ensure_read](../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_read.md)
- [ensure_write](../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_write.md)
- [ensure_delete](../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_delete.md)
- [ensure_share](../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_share.md)
- [ensure_tree_admin](../../../../../functions/crates/lpe-storage/src/public_folders/types/ensure_tree_admin.md)
- [public_folder_select_sql](../../../../../functions/crates/lpe-storage/src/public_folders/types/public_folder_select_sql.md)
- [public_folder_item_select_sql](../../../../../functions/crates/lpe-storage/src/public_folders/types/public_folder_item_select_sql.md)
- [map_public_folder_tree](../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder_tree.md)
- [map_public_folder](../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder.md)
- [map_public_folder_item](../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder_item.md)
- [map_public_folder_permission](../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder_permission.md)
- [map_public_folder_replica](../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder_replica.md)
- [map_public_folder_per_user_state](../../../../../functions/crates/lpe-storage/src/public_folders/types/map_public_folder_per_user_state.md)

# Imports

- `anyhow::{bail, Result}`
- `serde::{Deserialize, Serialize}`
- `uuid::Uuid`
- `crate::{
    PublicFolderItemRow, PublicFolderPerUserStateRow, PublicFolderPermissionRow,
    PublicFolderReplicaRow, PublicFolderRow, PublicFolderTreeRow,
}`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)