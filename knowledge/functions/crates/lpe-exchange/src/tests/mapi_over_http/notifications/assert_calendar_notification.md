---
type: Rust Function
title: assert_calendar_notification
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L84-L111
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql
---

# Signature

`fn assert_calendar_notification( poll: &MapiNotificationPoll, cursor: i64, event_mask: u16, folder_id: u64, message_id: u64, calendar_id: Uuid, event_id: Uuid, )`

# Called by

- [mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql.md)