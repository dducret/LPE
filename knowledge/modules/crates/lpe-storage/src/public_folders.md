---
type: Rust Module
title: public_folders
resource: crates/lpe-storage/src/public_folders.rs#L1-L1379
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-json-json
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-collaboration-validate-collaboration-rights-auditentryinput-publicfolderitemrow-publicfolderperuserstaterow-publicfolderpermissionrow-publicfolderreplicarow-publicfolderrow-publicfoldertreerow-storage
  - external/pub-crate-use-types-ensure-delete-ensure-read-ensure-share-ensure-tree-admin-ensure-write-map-public-folder-map-public-folder-item-map-public-folder-per-user-state-map-public-folder-permission-map-public-folder-replica-map-public-folder-tree-public-folder-item-select-sql-public-folder-select-sql-publicfolderaccess
  - external/pub-use-types-createpublicfolderinput-createpublicfoldertreeinput-publicfolder-publicfolderitem-publicfolderperuserstate-publicfolderperuserstatepatch-publicfolderpermission-publicfolderpermissioninput-publicfolderreplica-publicfolderreplicainput-publicfolderrights-publicfoldertree-updatepublicfolderinput-upsertpublicfolderiteminput
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [create_public_folder_tree](../../../../functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_tree.md)
- [create_public_folder_child](../../../../functions/crates/lpe-storage/src/public_folders/Storage/create_public_folder_child.md)
- [fetch_public_folder_trees](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_trees.md)
- [fetch_public_folder](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder.md)
- [fetch_public_folder_children](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_children.md)
- [update_public_folder](../../../../functions/crates/lpe-storage/src/public_folders/Storage/update_public_folder.md)
- [delete_public_folder](../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder.md)
- [fetch_public_folder_items](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items.md)
- [fetch_public_folder_items_by_ids](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_items_by_ids.md)
- [upsert_public_folder_item](../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_item.md)
- [delete_public_folder_item](../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_item.md)
- [fetch_public_folder_permissions](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permissions.md)
- [upsert_public_folder_permission](../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_permission.md)
- [delete_public_folder_permission](../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_permission.md)
- [fetch_public_folder_replicas](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_replicas.md)
- [upsert_public_folder_replica](../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_replica.md)
- [delete_public_folder_replica](../../../../functions/crates/lpe-storage/src/public_folders/Storage/delete_public_folder_replica.md)
- [fetch_public_folder_per_user_state](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_per_user_state.md)
- [patch_public_folder_per_user_state](../../../../functions/crates/lpe-storage/src/public_folders/Storage/patch_public_folder_per_user_state.md)
- [public_folder_access](../../../../functions/crates/lpe-storage/src/public_folders/Storage/public_folder_access.md)
- [fetch_public_folder_row](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_row.md)
- [fetch_public_folder_permission](../../../../functions/crates/lpe-storage/src/public_folders/Storage/fetch_public_folder_permission.md)

# Imports

- `anyhow::{bail, Result}`
- `serde_json::json`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    collaboration::validate_collaboration_rights, AuditEntryInput, PublicFolderItemRow,
    PublicFolderPerUserStateRow, PublicFolderPermissionRow, PublicFolderReplicaRow,
    PublicFolderRow, PublicFolderTreeRow, Storage,
}`
- `pub(crate) use types::{
    ensure_delete, ensure_read, ensure_share, ensure_tree_admin, ensure_write, map_public_folder,
    map_public_folder_item, map_public_folder_per_user_state, map_public_folder_permission,
    map_public_folder_replica, map_public_folder_tree, public_folder_item_select_sql,
    public_folder_select_sql, PublicFolderAccess,
}`
- `pub use types::{
    CreatePublicFolderInput, CreatePublicFolderTreeInput, PublicFolder, PublicFolderItem,
    PublicFolderPerUserState, PublicFolderPerUserStatePatch, PublicFolderPermission,
    PublicFolderPermissionInput, PublicFolderReplica, PublicFolderReplicaInput, PublicFolderRights,
    PublicFolderTree, UpdatePublicFolderInput, UpsertPublicFolderItemInput,
}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)