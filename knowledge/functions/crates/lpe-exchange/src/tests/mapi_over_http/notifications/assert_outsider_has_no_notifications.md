---
type: Rust Function
title: assert_outsider_has_no_notifications
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L168-L179
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql
---

# Signature

`async fn assert_outsider_has_no_notifications( storage: &Storage, outsider_account_id: Uuid, after_cursor: i64, ) -> anyhow::Result<()>`

# Calls

- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)

# Called by

- [mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql.md)