---
type: Rust Function
title: notification_event_input
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L4-L34
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql
---

# Signature

`fn notification_event_input( account_id: Uuid, event_id: Uuid, uid: &str, title: &str, sequence: i32, ) -> UpsertClientEventInput`

# Called by

- [mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql.md)
- [mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql.md)