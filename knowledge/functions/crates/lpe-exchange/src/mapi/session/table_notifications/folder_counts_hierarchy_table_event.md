---
type: Rust Function
title: folder_counts_hierarchy_table_event
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L290-L305
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/pending_collaboration_hierarchy_notification_requires_contents
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target
---

# Signature

`fn folder_counts_hierarchy_table_event( source_event: &MapiNotificationEvent, folder_event: &MapiNotificationEvent, ) -> Option<MapiNotificationEvent>`

# Called by

- [pending_collaboration_hierarchy_notification_requires_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/pending_collaboration_hierarchy_notification_requires_contents.md)
- [take_pending_notification_delivery_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/take_pending_notification_delivery_batch.md)
- [has_notification_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target.md)