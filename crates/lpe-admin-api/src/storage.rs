use axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use lpe_storage::{
    AuditEntryInput, AuthenticatedAdmin, NewStoragePool, Storage, StorageCleanupVisibilityResponse,
    StorageHealthResponse, StorageMigrationVisibilityResponse, StoragePolicyOverview,
    StoragePolicyUpdate, StoragePoolSummary, UpdateStoragePool,
};
use uuid::Uuid;

use crate::{
    http::{bad_request_error, internal_error},
    require_admin,
    types::{
        ApiResult, CreateStoragePoolRequest, UpdateStoragePolicyRequest, UpdateStoragePoolRequest,
    },
};

pub(crate) async fn list_storage_pools(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<Vec<StoragePoolSummary>> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    Ok(Json(
        storage
            .list_storage_pools(is_global_storage_admin(&admin))
            .await
            .map_err(internal_error)?,
    ))
}

pub(crate) async fn create_storage_pool(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<CreateStoragePoolRequest>,
) -> ApiResult<StoragePoolSummary> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    ensure_global_storage_admin(&admin)?;
    Ok(Json(
        storage
            .create_storage_pool(
                NewStoragePool {
                    name: request.name,
                    pool_kind: request.pool_kind,
                    status: request.status,
                },
                storage_audit(&admin, "create-storage-pool", "storage pool"),
            )
            .await
            .map_err(storage_policy_error)?,
    ))
}

pub(crate) async fn update_storage_pool(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(pool_id): AxumPath<Uuid>,
    Json(request): Json<UpdateStoragePoolRequest>,
) -> ApiResult<StoragePoolSummary> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    ensure_global_storage_admin(&admin)?;
    Ok(Json(
        storage
            .update_storage_pool(
                UpdateStoragePool {
                    pool_id,
                    name: request.name,
                    status: request.status,
                },
                storage_audit(&admin, "update-storage-pool", &pool_id.to_string()),
            )
            .await
            .map_err(storage_policy_error)?,
    ))
}

pub(crate) async fn get_storage_policies(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<StoragePolicyOverview> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    if is_global_storage_admin(&admin) {
        return Ok(Json(
            storage
                .fetch_platform_storage_policy_overview()
                .await
                .map_err(internal_error)?,
        ));
    }

    let tenant_id = admin_tenant_id(&admin)?;
    ensure_tenant_storage_admin(&admin, tenant_id)?;
    Ok(Json(
        storage
            .fetch_tenant_storage_policy_overview(tenant_id)
            .await
            .map_err(internal_error)?,
    ))
}

pub(crate) async fn get_storage_health(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<StorageHealthResponse> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    if is_global_storage_admin(&admin) {
        return Ok(Json(
            storage
                .fetch_platform_storage_health()
                .await
                .map_err(internal_error)?,
        ));
    }

    let tenant_id = admin_tenant_id(&admin)?;
    ensure_tenant_storage_admin(&admin, tenant_id)?;
    Ok(Json(
        storage
            .fetch_tenant_storage_health(tenant_id)
            .await
            .map_err(internal_error)?,
    ))
}

pub(crate) async fn get_storage_migrations(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<StorageMigrationVisibilityResponse> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    if is_global_storage_admin(&admin) {
        return Ok(Json(
            storage
                .fetch_platform_storage_migrations()
                .await
                .map_err(internal_error)?,
        ));
    }

    let tenant_id = admin_tenant_id(&admin)?;
    ensure_tenant_storage_admin(&admin, tenant_id)?;
    Ok(Json(
        storage
            .fetch_tenant_storage_migrations(tenant_id)
            .await
            .map_err(internal_error)?,
    ))
}

pub(crate) async fn get_storage_cleanup(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<StorageCleanupVisibilityResponse> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    if is_global_storage_admin(&admin) {
        return Ok(Json(
            storage
                .fetch_platform_storage_cleanup()
                .await
                .map_err(internal_error)?,
        ));
    }

    let tenant_id = admin_tenant_id(&admin)?;
    ensure_tenant_storage_admin(&admin, tenant_id)?;
    Ok(Json(
        storage
            .fetch_tenant_storage_cleanup(tenant_id)
            .await
            .map_err(internal_error)?,
    ))
}

pub(crate) async fn update_platform_storage_policy(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpdateStoragePolicyRequest>,
) -> ApiResult<StoragePolicyOverview> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    ensure_global_storage_admin(&admin)?;
    storage
        .set_platform_storage_policy(
            StoragePolicyUpdate {
                storage_pool_id: request.storage_pool_id,
            },
            storage_audit(&admin, "update-platform-storage-policy", "platform"),
        )
        .await
        .map_err(storage_policy_error)?;
    Ok(Json(
        storage
            .fetch_platform_storage_policy_overview()
            .await
            .map_err(internal_error)?,
    ))
}

pub(crate) async fn update_tenant_storage_policy(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(tenant_id): AxumPath<Uuid>,
    Json(request): Json<UpdateStoragePolicyRequest>,
) -> ApiResult<StoragePolicyOverview> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    ensure_tenant_storage_admin(&admin, tenant_id)?;
    storage
        .set_tenant_storage_policy(
            tenant_id,
            StoragePolicyUpdate {
                storage_pool_id: request.storage_pool_id,
            },
            storage_audit(
                &admin,
                "update-tenant-storage-policy",
                &tenant_id.to_string(),
            ),
        )
        .await
        .map_err(storage_policy_error)?;
    storage_policy_response_for_admin(&storage, &admin).await
}

pub(crate) async fn update_domain_storage_policy(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(domain_id): AxumPath<Uuid>,
    Json(request): Json<UpdateStoragePolicyRequest>,
) -> ApiResult<StoragePolicyOverview> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    let tenant_id = storage
        .storage_policy_tenant_for_domain(domain_id)
        .await
        .map_err(storage_policy_error)?;
    ensure_tenant_storage_admin(&admin, tenant_id)?;
    storage
        .set_domain_storage_policy(
            domain_id,
            StoragePolicyUpdate {
                storage_pool_id: request.storage_pool_id,
            },
            storage_audit(
                &admin,
                "update-domain-storage-policy",
                &domain_id.to_string(),
            ),
        )
        .await
        .map_err(storage_policy_error)?;
    storage_policy_response_for_admin(&storage, &admin).await
}

pub(crate) async fn update_account_storage_policy(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(account_id): AxumPath<Uuid>,
    Json(request): Json<UpdateStoragePolicyRequest>,
) -> ApiResult<StoragePolicyOverview> {
    let admin = require_admin(&storage, &headers, "policies").await?;
    let (tenant_id, _) = storage
        .storage_policy_tenant_and_domain_for_account(account_id)
        .await
        .map_err(storage_policy_error)?;
    ensure_tenant_storage_admin(&admin, tenant_id)?;
    storage
        .set_account_storage_policy(
            account_id,
            StoragePolicyUpdate {
                storage_pool_id: request.storage_pool_id,
            },
            storage_audit(
                &admin,
                "update-account-storage-policy",
                &account_id.to_string(),
            ),
        )
        .await
        .map_err(storage_policy_error)?;
    storage_policy_response_for_admin(&storage, &admin).await
}

async fn storage_policy_response_for_admin(
    storage: &Storage,
    admin: &AuthenticatedAdmin,
) -> ApiResult<StoragePolicyOverview> {
    if is_global_storage_admin(admin) {
        return Ok(Json(
            storage
                .fetch_platform_storage_policy_overview()
                .await
                .map_err(internal_error)?,
        ));
    }
    let tenant_id = admin_tenant_id(admin)?;
    Ok(Json(
        storage
            .fetch_tenant_storage_policy_overview(tenant_id)
            .await
            .map_err(internal_error)?,
    ))
}

fn storage_audit(admin: &AuthenticatedAdmin, action: &str, subject: &str) -> AuditEntryInput {
    AuditEntryInput {
        actor: admin.email.clone(),
        action: action.to_string(),
        subject: subject.to_string(),
    }
}

fn is_global_storage_admin(admin: &AuthenticatedAdmin) -> bool {
    admin.permissions.iter().any(|permission| permission == "*")
        || matches!(
            admin.role.as_str(),
            "server-admin" | "super-admin" | "global_admin"
        )
}

fn ensure_global_storage_admin(
    admin: &AuthenticatedAdmin,
) -> std::result::Result<(), (StatusCode, String)> {
    if is_global_storage_admin(admin) {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "global administrator rights are required".to_string(),
        ))
    }
}

fn ensure_tenant_storage_admin(
    admin: &AuthenticatedAdmin,
    tenant_id: Uuid,
) -> std::result::Result<(), (StatusCode, String)> {
    if is_global_storage_admin(admin) {
        return Ok(());
    }
    if matches!(admin.role.as_str(), "tenant-admin" | "tenant_admin")
        && admin_tenant_id(admin)? == tenant_id
    {
        return Ok(());
    }
    Err((
        StatusCode::FORBIDDEN,
        "tenant administrator cannot manage this storage policy scope".to_string(),
    ))
}

fn admin_tenant_id(admin: &AuthenticatedAdmin) -> std::result::Result<Uuid, (StatusCode, String)> {
    Uuid::parse_str(&admin.tenant_id).map_err(|_| {
        (
            StatusCode::FORBIDDEN,
            "admin tenant scope is invalid".to_string(),
        )
    })
}

fn storage_policy_error(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("not found")
        || lowered.contains("requires")
        || lowered.contains("unsupported")
        || lowered.contains("only postgresql")
        || lowered.contains("must reference")
        || lowered.contains("cannot disable")
        || lowered.contains("must be lowercase")
    {
        return bad_request_error(message);
    }
    internal_error(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin(role: &str, tenant_id: Uuid, permissions: Vec<&str>) -> AuthenticatedAdmin {
        AuthenticatedAdmin {
            tenant_id: tenant_id.to_string(),
            email: "admin@example.test".to_string(),
            display_name: "Admin".to_string(),
            role: role.to_string(),
            domain_id: None,
            domain_name: "All domains".to_string(),
            rights_summary: permissions.join(", "),
            permissions: permissions.into_iter().map(ToString::to_string).collect(),
            auth_method: "password".to_string(),
            expires_at: "2026-05-13T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn global_admin_can_manage_platform_storage_policy() {
        let tenant_id = Uuid::new_v4();
        let admin = admin("server-admin", tenant_id, vec!["*"]);
        assert!(ensure_global_storage_admin(&admin).is_ok());
        assert!(ensure_tenant_storage_admin(&admin, Uuid::new_v4()).is_ok());
    }

    #[test]
    fn tenant_admin_is_limited_to_own_tenant_storage_policy() {
        let tenant_id = Uuid::new_v4();
        let admin = admin("tenant-admin", tenant_id, vec!["dashboard", "policies"]);
        assert!(ensure_global_storage_admin(&admin).is_err());
        assert!(ensure_tenant_storage_admin(&admin, tenant_id).is_ok());
        assert!(ensure_tenant_storage_admin(&admin, Uuid::new_v4()).is_err());
    }

    #[test]
    fn storage_visibility_uses_global_or_own_tenant_scope() {
        let tenant_id = Uuid::new_v4();
        let global = admin("server-admin", Uuid::new_v4(), vec!["*"]);
        let tenant = admin("tenant-admin", tenant_id, vec!["dashboard", "policies"]);
        let domain = admin("domain-admin", tenant_id, vec!["dashboard", "policies"]);

        assert!(ensure_tenant_storage_admin(&global, tenant_id).is_ok());
        assert!(ensure_tenant_storage_admin(&tenant, tenant_id).is_ok());
        assert!(ensure_tenant_storage_admin(&tenant, Uuid::new_v4()).is_err());
        assert!(ensure_tenant_storage_admin(&domain, tenant_id).is_err());
    }

    #[test]
    fn storage_policy_errors_map_validation_to_bad_request() {
        let (status, _) = storage_policy_error(anyhow::anyhow!(
            "storage policy must reference an active PostgreSQL storage pool"
        ));
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
