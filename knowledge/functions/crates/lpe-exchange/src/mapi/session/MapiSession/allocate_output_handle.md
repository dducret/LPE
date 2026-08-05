---
type: Rust Method
title: allocate_output_handle
resource: crates/lpe-exchange/src/mapi/session.rs#L1026-L1032
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle_avoiding
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_prefers_free_low_output_slot_handle
  - functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_skips_reserved_same_execute_handle
  - functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_does_not_reuse_old_low_slot_handle
---

# Signature

`pub(in crate::mapi) fn allocate_output_handle( &mut self, output_handle_index: Option<u8>, object: MapiObject, ) -> u32`

# Calls

- [allocate_output_handle_avoiding](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle_avoiding.md)

# Called by

- [append_get_attachment_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response.md)
- [append_open_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response.md)
- [append_create_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)
- [append_open_embedded_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response.md)
- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [allocate_logon_response_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/allocate_logon_response_context.md)
- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [append_create_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response.md)
- [append_register_notification_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)
- [append_get_permissions_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response.md)
- [append_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)
- [append_clone_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response.md)
- [append_get_rules_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response.md)
- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [append_fast_transfer_source_copy_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response.md)
- [append_fast_transfer_source_copy_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response.md)
- [append_synchronization_get_transfer_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)
- [append_fast_transfer_destination_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_configure_response.md)
- [append_synchronization_open_collector_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [allocate_output_handle_prefers_free_low_output_slot_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_prefers_free_low_output_slot_handle.md)
- [allocate_output_handle_skips_reserved_same_execute_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_skips_reserved_same_execute_handle.md)
- [allocate_output_handle_does_not_reuse_old_low_slot_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_does_not_reuse_old_low_slot_handle.md)