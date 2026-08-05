---
type: Rust Module
title: storage
resource: crates/lpe-admin-api/src/storage.rs#L1-L520
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-path-as-axumpath-state-http-headermap-statuscode-json
  - external/lpe-storage-auditentryinput-authenticatedadmin-newstoragepool-storage-storagecleanupvisibilityresponse-storagehealthresponse-storagemigrationvisibilityresponse-storagepolicyoverview-storagepolicyupdate-storagepoolsummary-updatestoragepool
  - external/uuid-uuid
  - external/crate-http-bad-request-error-internal-error-require-admin-types-apiresult-createstoragepoolrequest-updatestoragepolicyrequest-updatestoragepoolrequest
  - external/super
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [list_storage_pools](../../../../functions/crates/lpe-admin-api/src/storage/list_storage_pools.md)
- [create_storage_pool](../../../../functions/crates/lpe-admin-api/src/storage/create_storage_pool.md)
- [update_storage_pool](../../../../functions/crates/lpe-admin-api/src/storage/update_storage_pool.md)
- [get_storage_policies](../../../../functions/crates/lpe-admin-api/src/storage/get_storage_policies.md)
- [get_storage_health](../../../../functions/crates/lpe-admin-api/src/storage/get_storage_health.md)
- [get_storage_migrations](../../../../functions/crates/lpe-admin-api/src/storage/get_storage_migrations.md)
- [get_storage_cleanup](../../../../functions/crates/lpe-admin-api/src/storage/get_storage_cleanup.md)
- [update_platform_storage_policy](../../../../functions/crates/lpe-admin-api/src/storage/update_platform_storage_policy.md)
- [update_tenant_storage_policy](../../../../functions/crates/lpe-admin-api/src/storage/update_tenant_storage_policy.md)
- [update_domain_storage_policy](../../../../functions/crates/lpe-admin-api/src/storage/update_domain_storage_policy.md)
- [update_account_storage_policy](../../../../functions/crates/lpe-admin-api/src/storage/update_account_storage_policy.md)
- [storage_policy_response_for_admin](../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_response_for_admin.md)
- [storage_audit](../../../../functions/crates/lpe-admin-api/src/storage/storage_audit.md)
- [storage_policy_audit](../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit.md)
- [is_global_storage_admin](../../../../functions/crates/lpe-admin-api/src/storage/is_global_storage_admin.md)
- [ensure_global_storage_admin](../../../../functions/crates/lpe-admin-api/src/storage/ensure_global_storage_admin.md)
- [ensure_tenant_storage_admin](../../../../functions/crates/lpe-admin-api/src/storage/ensure_tenant_storage_admin.md)
- [admin_tenant_id](../../../../functions/crates/lpe-admin-api/src/storage/admin_tenant_id.md)
- [storage_policy_error](../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_error.md)
- [admin](../../../../functions/crates/lpe-admin-api/src/storage/admin.md)
- [global_admin_can_manage_platform_storage_policy](../../../../functions/crates/lpe-admin-api/src/storage/global_admin_can_manage_platform_storage_policy.md)
- [tenant_admin_is_limited_to_own_tenant_storage_policy](../../../../functions/crates/lpe-admin-api/src/storage/tenant_admin_is_limited_to_own_tenant_storage_policy.md)
- [storage_visibility_uses_global_or_own_tenant_scope](../../../../functions/crates/lpe-admin-api/src/storage/storage_visibility_uses_global_or_own_tenant_scope.md)
- [storage_policy_audit_records_scope_and_pool_target](../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_scope_and_pool_target.md)
- [storage_policy_audit_records_inheritance_clear](../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_audit_records_inheritance_clear.md)
- [storage_policy_errors_map_validation_to_bad_request](../../../../functions/crates/lpe-admin-api/src/storage/storage_policy_errors_map_validation_to_bad_request.md)
- [create_storage_pool_request_accepts_s3_compatible_config_shape](../../../../functions/crates/lpe-admin-api/src/storage/create_storage_pool_request_accepts_s3_compatible_config_shape.md)

# Imports

- `axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    Json,
}`
- `lpe_storage::{
    AuditEntryInput, AuthenticatedAdmin, NewStoragePool, Storage, StorageCleanupVisibilityResponse,
    StorageHealthResponse, StorageMigrationVisibilityResponse, StoragePolicyOverview,
    StoragePolicyUpdate, StoragePoolSummary, UpdateStoragePool,
}`
- `uuid::Uuid`
- `crate::{
    http::{bad_request_error, internal_error},
    require_admin,
    types::{
        ApiResult, CreateStoragePoolRequest, UpdateStoragePolicyRequest, UpdateStoragePoolRequest,
    },
}`
- `super::*`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)