---
type: Rust Method
title: take_pending_notification_delivery_batch
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L111-L226
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_changed_event
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_modified_event
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_hierarchy_table_event
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/is_complete_for_wire
  - functions/crates/lpe-exchange/src/mapi/notifications/registration_matches_event
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_matches_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_new_mail_hierarchy_row_survives_preceding_basic_table_change
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_collaboration_content_changes_for_active_root_depth_hierarchy_table_without_counts
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_derives_counted_folder_modified_notification_for_collaboration_content_create
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions
  - functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables
  - functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify
---

# Signature

`pub(in crate::mapi) fn take_pending_notification_delivery_batch( &mut self, ) -> ( Vec<(u32, u8, MapiNotificationEvent)>, VecDeque<MapiNotificationEvent>, )`

# Calls

- [table_changed_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_changed_event.md)
- [folder_counts_modified_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_modified_event.md)
- [folder_counts_hierarchy_table_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/folder_counts_hierarchy_table_event.md)
- [is_complete_for_wire](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/is_complete_for_wire.md)
- [registration_matches_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/registration_matches_event.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [table_matches_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/table_matches_event.md)

# Called by

- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [execute_overflow_restores_deliverable_notification_batch](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch.md)
- [execute_overflow_does_not_restore_unmatched_notification](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification.md)
- [session_retains_folder_count_change_for_active_parent_hierarchy_table](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table.md)
- [session_new_mail_hierarchy_row_survives_preceding_basic_table_change](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_new_mail_hierarchy_row_survives_preceding_basic_table_change.md)
- [session_retains_collaboration_content_changes_for_active_root_depth_hierarchy_table_without_counts](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_collaboration_content_changes_for_active_root_depth_hierarchy_table_without_counts.md)
- [session_derives_counted_folder_modified_notification_for_collaboration_content_create](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_derives_counted_folder_modified_notification_for_collaboration_content_create.md)
- [session_delivers_only_complete_message_moves_and_copies_to_subscriptions](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions.md)
- [hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables.md)
- [notification_subscription_preserves_rop_logon_id_through_rop_notify](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify.md)