---
type: Rust Function
title: restore_recoverable_item
resource: crates/lpe-admin-api/src/workspace.rs#L557-L564
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/restore_recoverable_item_with_store
---

# Signature

`pub(crate) async fn restore_recoverable_item( State(storage): State<Storage>, headers: HeaderMap, AxumPath(recoverable_item_id): AxumPath<Uuid>, Json(request): Json<RestoreRecoverableItemRequest>, ) -> ApiResult<JmapEmail>`

# Calls

- [restore_recoverable_item_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/restore_recoverable_item_with_store.md)