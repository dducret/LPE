---
type: Rust Function
title: postgres_mapi_hierarchy_sync_returns_every_retained_folder_tombstone
resource: crates/lpe-exchange/src/tests/hierarchy_tombstones.rs#L6-L152
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes
---

# Signature

`async fn postgres_mapi_hierarchy_sync_returns_every_retained_folder_tombstone()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [fetch_mapi_sync_changes](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes.md)