---
type: Rust Function
title: mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L182-L365
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/insert_notification_account
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_audit
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
---

# Signature

`async fn mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql( ) -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [insert_notification_account](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/insert_notification_account.md)
- [ensure_jmap_system_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [fetch_mapi_notification_cursor](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [postgres_mapi_audit](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_audit.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)