---
type: Rust Function
title: mapi_navigation_shortcut_upsert_preserves_distinct_message_rows
resource: crates/lpe-exchange/src/tests/mod.rs#L1079-L1321
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut
---

# Signature

`async fn mapi_navigation_shortcut_upsert_preserves_distinct_message_rows()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [upsert_mapi_navigation_shortcut](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut.md)
- [default_wlink_group_uuid](../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_uuid.md)
- [fetch_or_allocate_mapi_identities](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [virtual_special_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [fetch_mapi_sync_changes](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes.md)
- [delete_mapi_navigation_shortcut](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut.md)