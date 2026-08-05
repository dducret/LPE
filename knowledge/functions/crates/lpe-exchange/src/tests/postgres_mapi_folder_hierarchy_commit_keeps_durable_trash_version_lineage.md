---
type: Rust Function
title: postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage
resource: crates/lpe-exchange/src/tests/mod.rs#L3552-L3852
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes
  - functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_id
---

# Signature

`async fn postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [ensure_jmap_system_mailboxes](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [fetch_or_allocate_mapi_identities](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [fetch_mapi_sync_changes](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes.md)
- [test_merge_mapi_predecessor_change_lists](../../../../../functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists.md)
- [commit_mapi_folder_hierarchy_change](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change.md)
- [poll_mapi_notifications](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)
- [virtual_special_mailbox_id](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_id.md)