---
type: Rust Method
title: notification_test_shape
resource: crates/lpe-exchange/src/mapi/notifications.rs#L269-L289
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_custom_calendar_collection_lifecycle_replays_as_hierarchy_notifications_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql
---

# Signature

`pub(crate) fn notification_test_shape( &self, ) -> ( MapiNotificationKind, u16, u64, Option<u64>, Option<u64>, Option<u64>, Option<&'static str>, )`

# Called by

- [mapi_custom_calendar_collection_lifecycle_replays_as_hierarchy_notifications_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_custom_calendar_collection_lifecycle_replays_as_hierarchy_notifications_in_postgresql.md)
- [mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql.md)