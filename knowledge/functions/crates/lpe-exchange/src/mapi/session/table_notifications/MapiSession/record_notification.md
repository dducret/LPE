---
type: Rust Method
title: record_notification
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L72-L76
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions
  - functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables
  - functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending
---

# Signature

`pub(in crate::mapi) fn record_notification(&mut self, event: MapiNotificationEvent)`

# Calls

- [has_notification_target](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/has_notification_target.md)

# Called by

- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [append_pending_associated_config_save_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response.md)
- [append_existing_associated_config_save_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)
- [save_pending_contact](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact.md)
- [save_pending_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [save_existing_event](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)
- [append_create_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [append_empty_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response.md)
- [append_delete_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response.md)
- [append_folder_move_copy_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response.md)
- [append_move_copy_messages_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)
- [append_save_changes_message_route_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [append_set_message_read_flag_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response.md)
- [append_set_read_flags_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)
- [append_delete_messages_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_pending_navigation_shortcut_save_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [append_existing_navigation_shortcut_save_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response.md)
- [execute_overflow_restores_deliverable_notification_batch](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch.md)
- [execute_overflow_does_not_restore_unmatched_notification](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification.md)
- [session_retains_folder_count_change_for_active_parent_hierarchy_table](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_retains_folder_count_change_for_active_parent_hierarchy_table.md)
- [session_delivers_only_complete_message_moves_and_copies_to_subscriptions](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_delivers_only_complete_message_moves_and_copies_to_subscriptions.md)
- [hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/hierarchy_move_notifies_the_source_subscription_and_refreshes_both_parent_tables.md)
- [notification_subscription_preserves_rop_logon_id_through_rop_notify](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify.md)
- [notification_wait_event_pending](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending.md)