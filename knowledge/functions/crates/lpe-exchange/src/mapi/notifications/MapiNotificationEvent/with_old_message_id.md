---
type: Rust Method
title: with_old_message_id
resource: crates/lpe-exchange/src/mapi/notifications.rs#L155-L158
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/object_moved_and_copied_notifications_preserve_source_message_id
  - functions/crates/lpe-exchange/src/mapi/notifications/incomplete_message_move_notifications_are_not_serialized
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
---

# Signature

`pub(crate) fn with_old_message_id(mut self, old_message_id: Option<u64>) -> Self`

# Called by

- [object_moved_and_copied_notifications_preserve_source_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/object_moved_and_copied_notifications_preserve_source_message_id.md)
- [incomplete_message_move_notifications_are_not_serialized](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/incomplete_message_move_notifications_are_not_serialized.md)
- [session_delivers_only_complete_message_moves_and_copies_to_subscriptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions.md)
- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)