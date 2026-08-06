---
type: Rust Function
title: mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L1423-L1710
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/insert_notification_account
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_audit
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/create_notification_mailbox
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/notification_test_shape
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint
---

# Signature

`async fn mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql( ) -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [insert_notification_account](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/insert_notification_account.md)
- [ensure_jmap_system_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes.md)
- [postgres_mapi_audit](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_audit.md)
- [create_notification_mailbox](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/create_notification_mailbox.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [fetch_mapi_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)
- [notification_test_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/notification_test_shape.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [store_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint.md)
- [fetch_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint.md)