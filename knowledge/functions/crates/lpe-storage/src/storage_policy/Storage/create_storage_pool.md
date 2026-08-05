---
type: Rust Method
title: create_storage_pool
resource: crates/lpe-storage/src/storage_policy.rs#L71-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_policy/normalize_storage_pool_name
  - functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_kind
  - functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config
  - functions/crates/lpe-storage/src/storage_policy/normalize_storage_pool_status
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/storage_policy/storage_pool_summary_from_row
---

# Signature

`pub async fn create_storage_pool( &self, input: NewStoragePool, audit: AuditEntryInput, ) -> Result<StoragePoolSummary>`

# Calls

- [normalize_storage_pool_name](../../../../../../functions/crates/lpe-storage/src/storage_policy/normalize_storage_pool_name.md)
- [normalize_storage_pool_kind](../../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_kind.md)
- [normalize_storage_pool_config](../../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config.md)
- [normalize_storage_pool_status](../../../../../../functions/crates/lpe-storage/src/storage_policy/normalize_storage_pool_status.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [storage_pool_summary_from_row](../../../../../../functions/crates/lpe-storage/src/storage_policy/storage_pool_summary_from_row.md)