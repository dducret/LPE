---
type: Rust Function
title: mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L1294-L1399
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_create
  - functions/crates/lpe-storage/src/submission/mime/input
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/navigation_shortcut_notification_cursor
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/assert_navigation_shortcut_notification
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut
---

# Signature

`async fn mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql( ) -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [fetch_mapi_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [commit_mapi_navigation_shortcut_create](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_create.md)
- [input](../../../../../../../functions/crates/lpe-storage/src/submission/mime/input.md)
- [navigation_shortcut_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/navigation_shortcut_notification_cursor.md)
- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)
- [assert_navigation_shortcut_notification](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/assert_navigation_shortcut_notification.md)
- [upsert_mapi_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_navigation_shortcut.md)
- [delete_mapi_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_mapi_navigation_shortcut.md)