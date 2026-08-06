---
type: Rust Function
title: mapi_navigation_shortcut_create_preserves_distinct_rows_for_same_target
resource: crates/lpe-exchange/src/tests/mod.rs#L1393-L1482
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_create
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn mapi_navigation_shortcut_create_preserves_distinct_rows_for_same_target()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [commit_mapi_navigation_shortcut_create](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_create.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)