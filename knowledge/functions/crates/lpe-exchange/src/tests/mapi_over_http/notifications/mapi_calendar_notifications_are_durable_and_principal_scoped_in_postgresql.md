---
type: Rust Function
title: mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L743-L1164
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/insert_notification_account
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/notification_event_input
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/calendar_notification_ids
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/assert_calendar_notification
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/assert_outsider_has_no_notifications
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql( ) -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [insert_notification_account](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/insert_notification_account.md)
- [notification_event_input](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/notification_event_input.md)
- [calendar_notification_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/calendar_notification_ids.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [fetch_mapi_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)
- [assert_calendar_notification](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/assert_calendar_notification.md)
- [assert_outsider_has_no_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/assert_outsider_has_no_notifications.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)