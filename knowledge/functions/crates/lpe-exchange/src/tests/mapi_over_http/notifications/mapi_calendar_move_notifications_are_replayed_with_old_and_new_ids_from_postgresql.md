---
type: Rust Function
title: mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L494-L603
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/notification_event_input
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/calendar_notification_ids
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
---

# Signature

`async fn mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql( ) -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [notification_event_input](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/notification_event_input.md)
- [calendar_notification_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/calendar_notification_ids.md)
- [ensure_jmap_system_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [fetch_mapi_sync_changes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_changes.md)
- [fetch_mapi_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)