---
type: Rust Function
title: assert_navigation_shortcut_notification
resource: crates/lpe-exchange/src/tests/mapi_over_http/notifications.rs#L113-L138
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql
---

# Signature

`fn assert_navigation_shortcut_notification( poll: &MapiNotificationPoll, cursor: i64, event_mask: u16, message_id: u64, shortcut_id: Uuid, )`

# Called by

- [mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql.md)