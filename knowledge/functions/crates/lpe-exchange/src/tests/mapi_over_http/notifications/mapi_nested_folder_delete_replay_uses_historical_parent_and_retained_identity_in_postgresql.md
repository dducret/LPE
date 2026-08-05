---
type: Rust Function
title: mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L1449-L1567
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/create_notification_mailbox
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_audit
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
---

# Signature

`async fn mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql( ) -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [create_notification_mailbox](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/create_notification_mailbox.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [fetch_mapi_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [postgres_mapi_audit](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_audit.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)