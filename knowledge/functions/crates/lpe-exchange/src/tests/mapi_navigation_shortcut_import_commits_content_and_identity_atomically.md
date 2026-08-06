---
type: Rust Function
title: mapi_navigation_shortcut_import_commits_content_and_identity_atomically
resource: crates/lpe-exchange/src/tests/mod.rs#L1484-L2091
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids
  - functions/crates/lpe-exchange/src/tests/test_filetime
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn mapi_navigation_shortcut_import_commits_content_and_identity_atomically()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [reserve_mapi_local_replica_ids](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids.md)
- [test_filetime](../../../../../functions/crates/lpe-exchange/src/tests/test_filetime.md)
- [commit_mapi_navigation_shortcut_import](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [fetch_or_allocate_mapi_identities](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [test_merge_mapi_predecessor_change_lists](../../../../../functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)