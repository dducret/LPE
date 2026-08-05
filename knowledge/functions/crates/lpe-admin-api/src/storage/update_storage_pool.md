---
type: Rust Function
title: update_storage_pool
resource: crates/lpe-admin-api/src/storage.rs#L57-L79
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/storage/ensure_global_storage_admin
  - functions/crates/lpe-admin-api/src/storage/storage_audit
---

# Signature

`pub(crate) async fn update_storage_pool( State(storage): State<Storage>, headers: HeaderMap, AxumPath(pool_id): AxumPath<Uuid>, Json(request): Json<UpdateStoragePoolRequest>, ) -> ApiResult<StoragePoolSummary>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [ensure_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/ensure_global_storage_admin.md)
- [storage_audit](../../../../../functions/crates/lpe-admin-api/src/storage/storage_audit.md)