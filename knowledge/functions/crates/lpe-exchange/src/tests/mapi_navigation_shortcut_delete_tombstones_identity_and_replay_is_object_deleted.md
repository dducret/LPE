---
type: Rust Function
title: mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted
resource: crates/lpe-exchange/src/tests/mod.rs#L2095-L2310
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids
  - functions/crates/lpe-exchange/src/tests/test_filetime
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn mapi_navigation_shortcut_delete_tombstones_identity_and_replay_is_object_deleted()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [reserve_mapi_local_replica_ids](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids.md)
- [test_filetime](../../../../../functions/crates/lpe-exchange/src/tests/test_filetime.md)
- [commit_mapi_navigation_shortcut_import](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import.md)
- [delete_mapi_navigation_shortcut](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)