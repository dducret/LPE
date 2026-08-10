---
type: Rust Method
title: content
resource: crates/lpe-exchange/src/mapi/notifications.rs#L47-L70
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_preserves_pending_table_notification_after_releasing_its_table
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification
---

# Signature

`pub(in crate::mapi) fn content(folder_id: u64, message_id: Option<u64>) -> Self`

# Called by

- [append_pending_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response.md)
- [append_existing_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)
- [save_pending_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_pending_contact.md)
- [save_existing_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact.md)
- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)
- [append_empty_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response.md)
- [append_move_copy_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [append_set_message_read_flag_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response.md)
- [append_set_read_flags_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)
- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [append_existing_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_existing_navigation_shortcut_save_response.md)
- [execute_preserves_pending_table_notification_after_releasing_its_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_preserves_pending_table_notification_after_releasing_its_table.md)
- [execute_overflow_restores_deliverable_notification_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch.md)
- [execute_overflow_does_not_restore_unmatched_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification.md)