---
type: Rust Function
title: purge_recoverable_item
resource: crates/lpe-admin-api/src/workspace.rs#L571-L577
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/purge_recoverable_item_with_store
---

# Signature

`pub(crate) async fn purge_recoverable_item( State(storage): State<Storage>, headers: HeaderMap, AxumPath(recoverable_item_id): AxumPath<Uuid>, ) -> ApiResult<HealthResponse>`

# Calls

- [purge_recoverable_item_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/purge_recoverable_item_with_store.md)