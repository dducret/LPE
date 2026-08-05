---
type: Rust Function
title: list_storage_pools
resource: crates/lpe-admin-api/src/storage.rs#L21-L32
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_admin
  - functions/crates/lpe-admin-api/src/storage/is_global_storage_admin
---

# Signature

`pub(crate) async fn list_storage_pools( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<StoragePoolSummary>>`

# Calls

- [require_admin](../../../../../functions/crates/lpe-admin-api/src/access/require_admin.md)
- [is_global_storage_admin](../../../../../functions/crates/lpe-admin-api/src/storage/is_global_storage_admin.md)