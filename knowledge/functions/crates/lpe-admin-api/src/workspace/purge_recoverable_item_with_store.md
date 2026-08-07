---
type: Rust Function
title: purge_recoverable_item_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L598-L620
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/purge_recoverable_item
  - functions/crates/lpe-admin-api/src/workspace/tests/recoverable_items_api_helpers_use_canonical_store_path
---

# Signature

`async fn purge_recoverable_item_with_store<S: ClientRecoverableStore>( storage: &S, headers: &HeaderMap, recoverable_item_id: Uuid, ) -> ApiResult<HealthResponse>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [purge_recoverable_item](../../../../../functions/crates/lpe-admin-api/src/workspace/purge_recoverable_item.md)
- [recoverable_items_api_helpers_use_canonical_store_path](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/recoverable_items_api_helpers_use_canonical_store_path.md)