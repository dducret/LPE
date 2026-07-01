use axum::{
    extract::{Path as AxumPath, State},
    http::HeaderMap,
    Json,
};
use lpe_storage::{
    AuditEntryInput, CreatePublicFolderInput, CreatePublicFolderTreeInput, HealthResponse,
    PublicFolder, PublicFolderItem, PublicFolderPerUserState, PublicFolderPerUserStatePatch,
    PublicFolderPermission, PublicFolderPermissionInput, PublicFolderReplica,
    PublicFolderReplicaInput, PublicFolderTree, Storage, UpdatePublicFolderInput,
    UpsertPublicFolderItemInput,
};
use uuid::Uuid;

use crate::{
    http::bad_request_error,
    require_account,
    types::{
        ApiResult, CreatePublicFolderRequest, CreatePublicFolderTreeRequest,
        PublicFolderPerUserStatePatchBatchRequest, PublicFolderPermissionRequest,
        PublicFolderReplicaRequest, UpdatePublicFolderRequest, UpsertPublicFolderItemRequest,
    },
};
pub(crate) async fn list_public_folder_trees(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<PublicFolderTree>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_public_folder_trees(account.account_id)
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn create_public_folder_tree(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreatePublicFolderTreeRequest>,
) -> ApiResult<PublicFolder> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .create_public_folder_tree(
                CreatePublicFolderTreeInput {
                    account_id: account.account_id,
                    display_name: request.display_name,
                },
                AuditEntryInput {
                    actor: account.email,
                    action: "create-public-folder-tree".to_string(),
                    subject: "public-folder-tree".to_string(),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn get_public_folder(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
) -> ApiResult<PublicFolder> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_public_folder(account.account_id, folder_id)
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn update_public_folder(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
    Json(request): Json<UpdatePublicFolderRequest>,
) -> ApiResult<PublicFolder> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .update_public_folder(
                UpdatePublicFolderInput {
                    account_id: account.account_id,
                    folder_id,
                    parent_folder_id: None,
                    display_name: request.display_name,
                    folder_class: request.folder_class,
                    sort_order: request.sort_order,
                },
                AuditEntryInput {
                    actor: account.email,
                    action: "update-public-folder".to_string(),
                    subject: folder_id.to_string(),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn delete_public_folder(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_public_folder(
            account.account_id,
            folder_id,
            AuditEntryInput {
                actor: account.email,
                action: "delete-public-folder".to_string(),
                subject: folder_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn list_public_folder_children(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
) -> ApiResult<Vec<PublicFolder>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_public_folder_children(account.account_id, folder_id)
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn create_public_folder_child(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
    Json(request): Json<CreatePublicFolderRequest>,
) -> ApiResult<PublicFolder> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .create_public_folder_child(
                CreatePublicFolderInput {
                    account_id: account.account_id,
                    parent_folder_id: folder_id,
                    display_name: request.display_name,
                    folder_class: request
                        .folder_class
                        .unwrap_or_else(|| "IPF.Note".to_string()),
                    sort_order: request.sort_order.unwrap_or(0),
                },
                AuditEntryInput {
                    actor: account.email,
                    action: "create-public-folder".to_string(),
                    subject: folder_id.to_string(),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn list_public_folder_items(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
) -> ApiResult<Vec<PublicFolderItem>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_public_folder_items(account.account_id, folder_id)
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn post_public_folder_item(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
    Json(request): Json<UpsertPublicFolderItemRequest>,
) -> ApiResult<PublicFolderItem> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .upsert_public_folder_item(
                map_public_folder_item_request(account.account_id, folder_id, request),
                AuditEntryInput {
                    actor: account.email,
                    action: "upsert-public-folder-item".to_string(),
                    subject: folder_id.to_string(),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn patch_public_folder_item(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((folder_id, item_id)): AxumPath<(Uuid, Uuid)>,
    Json(mut request): Json<UpsertPublicFolderItemRequest>,
) -> ApiResult<PublicFolderItem> {
    let account = require_account(&storage, &headers).await?;
    request.id = Some(item_id);
    Ok(Json(
        storage
            .upsert_public_folder_item(
                map_public_folder_item_request(account.account_id, folder_id, request),
                AuditEntryInput {
                    actor: account.email,
                    action: "update-public-folder-item".to_string(),
                    subject: item_id.to_string(),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn delete_public_folder_item(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((folder_id, item_id)): AxumPath<(Uuid, Uuid)>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_public_folder_item(
            account.account_id,
            folder_id,
            item_id,
            AuditEntryInput {
                actor: account.email,
                action: "delete-public-folder-item".to_string(),
                subject: item_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn list_public_folder_permissions(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
) -> ApiResult<Vec<PublicFolderPermission>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_public_folder_permissions(account.account_id, folder_id)
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn put_public_folder_permission(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((folder_id, principal_id)): AxumPath<(Uuid, Uuid)>,
    Json(request): Json<PublicFolderPermissionRequest>,
) -> ApiResult<PublicFolderPermission> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .upsert_public_folder_permission(
                PublicFolderPermissionInput {
                    account_id: account.account_id,
                    public_folder_id: folder_id,
                    principal_account_id: principal_id,
                    may_read: request.may_read,
                    may_write: request.may_write,
                    may_delete: request.may_delete,
                    may_share: request.may_share,
                },
                AuditEntryInput {
                    actor: account.email,
                    action: "upsert-public-folder-permission".to_string(),
                    subject: principal_id.to_string(),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn delete_public_folder_permission(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((folder_id, principal_id)): AxumPath<(Uuid, Uuid)>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_public_folder_permission(
            account.account_id,
            folder_id,
            principal_id,
            AuditEntryInput {
                actor: account.email,
                action: "delete-public-folder-permission".to_string(),
                subject: principal_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn list_public_folder_replicas(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
) -> ApiResult<Vec<PublicFolderReplica>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_public_folder_replicas(account.account_id, folder_id)
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn put_public_folder_replica(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
    Json(request): Json<PublicFolderReplicaRequest>,
) -> ApiResult<PublicFolderReplica> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .upsert_public_folder_replica(
                PublicFolderReplicaInput {
                    account_id: account.account_id,
                    public_folder_id: folder_id,
                    server_name: request.server_name,
                    sort_order: request.sort_order,
                },
                AuditEntryInput {
                    actor: account.email,
                    action: "upsert-public-folder-replica".to_string(),
                    subject: folder_id.to_string(),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn delete_public_folder_replica(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((folder_id, replica_id)): AxumPath<(Uuid, Uuid)>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_public_folder_replica(
            account.account_id,
            folder_id,
            replica_id,
            AuditEntryInput {
                actor: account.email,
                action: "delete-public-folder-replica".to_string(),
                subject: replica_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn list_public_folder_per_user_state(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
) -> ApiResult<Vec<PublicFolderPerUserState>> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .fetch_public_folder_per_user_state(account.account_id, folder_id)
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn patch_public_folder_per_user_state(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(folder_id): AxumPath<Uuid>,
    Json(request): Json<PublicFolderPerUserStatePatchBatchRequest>,
) -> ApiResult<Vec<PublicFolderPerUserState>> {
    let account = require_account(&storage, &headers).await?;
    let patches = request
        .updates
        .into_iter()
        .map(|update| PublicFolderPerUserStatePatch {
            item_id: update.item_id,
            is_read: update.is_read,
            last_seen_change: update.last_seen_change,
            private_json: update.private_json,
        })
        .collect::<Vec<_>>();
    Ok(Json(
        storage
            .patch_public_folder_per_user_state(account.account_id, folder_id, &patches)
            .await
            .map_err(bad_request_error)?,
    ))
}

fn map_public_folder_item_request(
    account_id: Uuid,
    public_folder_id: Uuid,
    request: UpsertPublicFolderItemRequest,
) -> UpsertPublicFolderItemInput {
    UpsertPublicFolderItemInput {
        id: request.id,
        account_id,
        public_folder_id,
        item_kind: request.item_kind.unwrap_or_else(|| "post".to_string()),
        message_class: request
            .message_class
            .unwrap_or_else(|| "IPM.Post".to_string()),
        subject: request.subject,
        body_text: request.body_text,
        body_html_sanitized: request.body_html_sanitized,
        source_payload_json: request
            .source_payload_json
            .unwrap_or_else(|| "{}".to_string()),
    }
}
