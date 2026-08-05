---
type: Rust Function
title: list_recoverable_items_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L524-L536
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/list_recoverable_items
  - functions/crates/lpe-admin-api/src/workspace/tests/recoverable_items_api_helpers_use_canonical_store_path
---

# Signature

`async fn list_recoverable_items_with_store<S: ClientRecoverableStore>( storage: &S, headers: &HeaderMap, request: RecoverableItemsQueryRequest, ) -> ApiResult<Vec<RecoverableItem>>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [list_recoverable_items](../../../../../functions/crates/lpe-admin-api/src/workspace/list_recoverable_items.md)
- [recoverable_items_api_helpers_use_canonical_store_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/recoverable_items_api_helpers_use_canonical_store_path.md)