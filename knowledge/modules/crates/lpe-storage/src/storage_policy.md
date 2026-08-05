---
type: Rust Module
title: storage_policy
resource: crates/lpe-storage/src/storage_policy.rs#L1-L1192
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/sqlx-row
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/crate-storage-backend-normalize-storage-pool-config-normalize-storage-pool-kind-select-storage-backend-storage-pool-config-summary-auditentryinput-newstoragepool-storage-storagepolicyoverview-storagepolicyscope-storagepolicysummary-storagepolicyupdate-storagepoolreference-storagepoolsummary-updatestoragepool-platform-tenant-id
  - external/super
  - external/crate-storage
  - external/serde-json-json
  - external/sqlx-postgres-pgpooloptions
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [PoolRow](../../../../classes/crates/lpe-storage/src/storage_policy/PoolRow.md)
- [AssignmentRow](../../../../classes/crates/lpe-storage/src/storage_policy/AssignmentRow.md)
- [TenantTarget](../../../../classes/crates/lpe-storage/src/storage_policy/TenantTarget.md)
- [DomainTarget](../../../../classes/crates/lpe-storage/src/storage_policy/DomainTarget.md)
- [AccountTarget](../../../../classes/crates/lpe-storage/src/storage_policy/AccountTarget.md)
- [list_storage_pools](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/list_storage_pools.md)
- [create_storage_pool](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool.md)
- [update_storage_pool](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool.md)
- [fetch_platform_storage_policy_overview](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_platform_storage_policy_overview.md)
- [fetch_tenant_storage_policy_overview](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_tenant_storage_policy_overview.md)
- [set_platform_storage_policy](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_platform_storage_policy.md)
- [set_tenant_storage_policy](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_tenant_storage_policy.md)
- [set_domain_storage_policy](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_domain_storage_policy.md)
- [set_account_storage_policy](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/set_account_storage_policy.md)
- [storage_policy_tenant_for_domain](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/storage_policy_tenant_for_domain.md)
- [storage_policy_tenant_and_domain_for_account](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/storage_policy_tenant_and_domain_for_account.md)
- [fetch_storage_policy_overview](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/fetch_storage_policy_overview.md)
- [load_storage_pool_rows](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_rows.md)
- [load_storage_pool_row](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_pool_row.md)
- [load_storage_policy_assignments](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_assignments.md)
- [load_storage_policy_tenants](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_tenants.md)
- [load_storage_policy_domains](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_domains.md)
- [load_storage_policy_accounts](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/load_storage_policy_accounts.md)
- [replace_storage_policy_assignment](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/replace_storage_policy_assignment.md)
- [ensure_tenant_exists](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_tenant_exists.md)
- [ensure_active_storage_pool](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_active_storage_pool.md)
- [ensure_storage_pool_can_be_disabled](../../../../functions/crates/lpe-storage/src/storage_policy/Storage/ensure_storage_pool_can_be_disabled.md)
- [normalize_storage_pool_name](../../../../functions/crates/lpe-storage/src/storage_policy/normalize_storage_pool_name.md)
- [normalize_storage_pool_status](../../../../functions/crates/lpe-storage/src/storage_policy/normalize_storage_pool_status.md)
- [pool_row_from_row](../../../../functions/crates/lpe-storage/src/storage_policy/pool_row_from_row.md)
- [storage_pool_summary_from_row](../../../../functions/crates/lpe-storage/src/storage_policy/storage_pool_summary_from_row.md)
- [storage_pool_summary](../../../../functions/crates/lpe-storage/src/storage_policy/storage_pool_summary.md)
- [storage_pool_reference](../../../../functions/crates/lpe-storage/src/storage_policy/storage_pool_reference.md)
- [assignment_row_from_row](../../../../functions/crates/lpe-storage/src/storage_policy/assignment_row_from_row.md)
- [assignment_key](../../../../functions/crates/lpe-storage/src/storage_policy/assignment_key.md)
- [assignment_pool](../../../../functions/crates/lpe-storage/src/storage_policy/assignment_pool.md)
- [policy_summary](../../../../functions/crates/lpe-storage/src/storage_policy/policy_summary.md)
- [test_storage](../../../../functions/crates/lpe-storage/src/storage_policy/test_storage.md)
- [audit](../../../../functions/crates/lpe-storage/src/storage_policy/audit.md)
- [insert_tenant_domain_account](../../../../functions/crates/lpe-storage/src/storage_policy/insert_tenant_domain_account.md)
- [create_secondary_pool](../../../../functions/crates/lpe-storage/src/storage_policy/create_secondary_pool.md)
- [tenant_domain_and_account_policy_inherit_and_clear](../../../../functions/crates/lpe-storage/src/storage_policy/tenant_domain_and_account_policy_inherit_and_clear.md)
- [policy_rejects_disabled_or_unknown_pool](../../../../functions/crates/lpe-storage/src/storage_policy/policy_rejects_disabled_or_unknown_pool.md)
- [s3_compatible_pool_config_is_redacted_in_summary](../../../../functions/crates/lpe-storage/src/storage_policy/s3_compatible_pool_config_is_redacted_in_summary.md)
- [policy_changes_do_not_create_migration_jobs](../../../../functions/crates/lpe-storage/src/storage_policy/policy_changes_do_not_create_migration_jobs.md)
- [policy_change_records_admin_audit_event](../../../../functions/crates/lpe-storage/src/storage_policy/policy_change_records_admin_audit_event.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `sqlx::Row`
- `std::collections::HashMap`
- `uuid::Uuid`
- `crate::{
    storage_backend::{
        normalize_storage_pool_config, normalize_storage_pool_kind, select_storage_backend,
        storage_pool_config_summary,
    },
    AuditEntryInput, NewStoragePool, Storage, StoragePolicyOverview, StoragePolicyScope,
    StoragePolicySummary, StoragePolicyUpdate, StoragePoolReference, StoragePoolSummary,
    UpdateStoragePool, PLATFORM_TENANT_ID,
}`
- `super::*`
- `crate::Storage`
- `serde_json::json`
- `sqlx::postgres::PgPoolOptions`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)