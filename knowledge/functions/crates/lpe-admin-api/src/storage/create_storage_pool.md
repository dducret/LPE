---
type: Rust Function
title: create_storage_pool
resource: crates/lpe-admin-api/src/storage.rs#L34-L55
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/storage/ensure_global_storage_admin
  - functions/crates/lpe-admin-api/src/storage/storage_audit
---

# Signature

`pub(crate) async fn create_storage_pool( State(storage): State<Storage>, headers: HeaderMap, Json(request): Json<CreateStoragePoolRequest>, ) -> ApiResult<StoragePoolSummary>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [ensure_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_global_storage_admin.md)
- [storage_audit](../../../../../functions/crates/lpe-admin-api/src/storage/storage_audit.md)