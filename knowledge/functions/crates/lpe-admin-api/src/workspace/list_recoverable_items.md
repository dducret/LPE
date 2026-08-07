---
type: Rust Function
title: list_recoverable_items
resource: crates/lpe-admin-api/src/workspace.rs#L535-L541
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/list_recoverable_items_with_store
---

# Signature

`pub(crate) async fn list_recoverable_items( State(storage): State<Storage>, headers: HeaderMap, Query(request): Query<RecoverableItemsQueryRequest>, ) -> ApiResult<Vec<RecoverableItem>>`

# Calls

- [list_recoverable_items_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/list_recoverable_items_with_store.md)