---
type: Rust Function
title: mapi_contact_notification_create_carries_current_total_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L1193-L1291
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/notification_test_shape
---

# Signature

`async fn mapi_contact_notification_create_carries_current_total_in_postgresql() -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [fetch_mapi_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)
- [notification_test_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/notification_test_shape.md)