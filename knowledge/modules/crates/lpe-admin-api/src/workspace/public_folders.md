---
type: Rust Module
title: public_folders
resource: crates/lpe-admin-api/src/workspace/public_folders.rs#L1-L450
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-path-as-axumpath-state-http-headermap-json
  - external/lpe-storage-auditentryinput-createpublicfolderinput-createpublicfoldertreeinput-healthresponse-publicfolder-publicfolderitem-publicfolderperuserstate-publicfolderperuserstatepatch-publicfolderpermission-publicfolderpermissioninput-publicfolderreplica-publicfolderreplicainput-publicfoldertree-storage-updatepublicfolderinput-upsertpublicfolderiteminput
  - external/uuid-uuid
  - external/crate-http-bad-request-error-require-account-types-apiresult-createpublicfolderrequest-createpublicfoldertreerequest-publicfolderperuserstatepatchbatchrequest-publicfolderpermissionrequest-publicfolderreplicarequest-updatepublicfolderrequest-upsertpublicfolderitemrequest
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [list_public_folder_trees](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_trees.md)
- [create_public_folder_tree](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/create_public_folder_tree.md)
- [get_public_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/get_public_folder.md)
- [update_public_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/update_public_folder.md)
- [delete_public_folder](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder.md)
- [list_public_folder_children](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_children.md)
- [create_public_folder_child](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/create_public_folder_child.md)
- [list_public_folder_items](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_items.md)
- [post_public_folder_item](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/post_public_folder_item.md)
- [patch_public_folder_item](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/patch_public_folder_item.md)
- [delete_public_folder_item](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder_item.md)
- [list_public_folder_permissions](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_permissions.md)
- [put_public_folder_permission](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/put_public_folder_permission.md)
- [delete_public_folder_permission](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder_permission.md)
- [list_public_folder_replicas](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_replicas.md)
- [put_public_folder_replica](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/put_public_folder_replica.md)
- [delete_public_folder_replica](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/delete_public_folder_replica.md)
- [list_public_folder_per_user_state](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/list_public_folder_per_user_state.md)
- [patch_public_folder_per_user_state](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/patch_public_folder_per_user_state.md)
- [map_public_folder_item_request](../../../../../functions/crates/lpe-admin-api/src/workspace/public_folders/map_public_folder_item_request.md)

# Imports

- `axum::{
    extract::{Path as AxumPath, State},
    http::HeaderMap,
    Json,
}`
- `lpe_storage::{
    AuditEntryInput, CreatePublicFolderInput, CreatePublicFolderTreeInput, HealthResponse,
    PublicFolder, PublicFolderItem, PublicFolderPerUserState, PublicFolderPerUserStatePatch,
    PublicFolderPermission, PublicFolderPermissionInput, PublicFolderReplica,
    PublicFolderReplicaInput, PublicFolderTree, Storage, UpdatePublicFolderInput,
    UpsertPublicFolderItemInput,
}`
- `uuid::Uuid`
- `crate::{
    http::bad_request_error,
    require_account,
    types::{
        ApiResult, CreatePublicFolderRequest, CreatePublicFolderTreeRequest,
        PublicFolderPerUserStatePatchBatchRequest, PublicFolderPermissionRequest,
        PublicFolderReplicaRequest, UpdatePublicFolderRequest, UpsertPublicFolderItemRequest,
    },
}`

# Member of

- [lpe-admin-api](../../../../../packages/crates/lpe-admin-api.md)