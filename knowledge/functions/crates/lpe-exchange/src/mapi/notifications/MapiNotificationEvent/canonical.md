---
type: Rust Method
title: canonical
resource: crates/lpe-exchange/src/mapi/notifications.rs#L97-L138
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_maps_logical_default_folder_ids_to_durable_ids
  - functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_with_message_id_encodes_exchange_zero_message_flags
  - functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags
  - functions/crates/lpe-exchange/src/mapi/notifications/object_moved_and_copied_notifications_preserve_source_message_id
  - functions/crates/lpe-exchange/src/mapi/notifications/incomplete_message_move_notifications_are_not_serialized
  - functions/crates/lpe-exchange/src/mapi/notifications/incomplete_hierarchy_move_notification_is_not_serialized
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_collaboration_content_changes_for_active_root_depth_hierarchy_table_without_counts
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions
  - functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event
---

# Signature

`pub(crate) fn canonical( kind: MapiNotificationKind, event_mask: u16, folder_id: u64, message_id: Option<u64>, old_folder_id: Option<u64>, change_cursor: i64, modseq: u64, total_messages: Option<i32>, unread_messages: Option<i32>, change_kind: String, display_name: Option<String>, parent_display_name: Option<String>, message_subject: Option<String>, message_class: Option<String>, ) -> Self`

# Called by

- [scoped_codec_maps_logical_default_folder_ids_to_durable_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_maps_logical_default_folder_ids_to_durable_ids.md)
- [new_mail_notification_with_message_id_encodes_exchange_zero_message_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_with_message_id_encodes_exchange_zero_message_flags.md)
- [new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags.md)
- [object_moved_and_copied_notifications_preserve_source_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/object_moved_and_copied_notifications_preserve_source_message_id.md)
- [incomplete_message_move_notifications_are_not_serialized](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/incomplete_message_move_notifications_are_not_serialized.md)
- [incomplete_hierarchy_move_notification_is_not_serialized](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/incomplete_hierarchy_move_notification_is_not_serialized.md)
- [session_retains_folder_count_change_for_active_parent_hierarchy_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table.md)
- [session_retains_collaboration_content_changes_for_active_root_depth_hierarchy_table_without_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_collaboration_content_changes_for_active_root_depth_hierarchy_table_without_counts.md)
- [session_delivers_only_complete_message_moves_and_copies_to_subscriptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions.md)
- [notification_subscription_preserves_rop_logon_id_through_rop_notify](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify.md)
- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)
- [mapi_calendar_notification_event](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event.md)