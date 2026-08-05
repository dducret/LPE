---
type: Rust Function
title: mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L368-L491
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_audit
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
---

# Signature

`async fn mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql( ) -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [ensure_jmap_system_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [fetch_mapi_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [postgres_mapi_audit](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_audit.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)