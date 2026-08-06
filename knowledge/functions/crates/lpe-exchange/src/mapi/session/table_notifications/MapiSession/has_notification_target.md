---
type: Rust Method
title: has_notification_target
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L238-L262
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_changed_event
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_modified_event
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_hierarchy_table_event
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/is_complete_for_wire
  - functions/crates/lpe-exchange/src/mapi/notifications/registration_matches_event
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_matches_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/matching_notifications
---

# Signature

`fn has_notification_target(&self, event: &MapiNotificationEvent) -> bool`

# Calls

- [table_changed_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_changed_event.md)
- [folder_counts_modified_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_modified_event.md)
- [folder_counts_hierarchy_table_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_hierarchy_table_event.md)
- [is_complete_for_wire](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/is_complete_for_wire.md)
- [registration_matches_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/registration_matches_event.md)
- [table_matches_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_matches_event.md)

# Called by

- [record_notification](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [matching_notifications](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/matching_notifications.md)