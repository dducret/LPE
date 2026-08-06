---
type: Rust Module
title: notifications
resource: crates/lpe-exchange/src/mapi/notifications.rs#L1-L964
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-identity-wire-id-bytes-from-object-id
  - external/super-rop
  - external/super-wire-mapinotificationeventmask-mapi-content-notification-mask-mapi-hierarchy-notification-mask
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [MapiNotificationRegistration](../../../../../classes/crates/lpe-exchange/src/mapi/notifications/MapiNotificationRegistration.md)
- [MapiNotificationKind](../../../../../classes/crates/lpe-exchange/src/mapi/notifications/MapiNotificationKind.md)
- [MapiNotificationEvent](../../../../../classes/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent.md)
- [content](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [hierarchy](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy.md)
- [canonical](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical.md)
- [with_canonical_ids](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_canonical_ids.md)
- [with_parent_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_parent_folder_id.md)
- [with_old_message_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_message_id.md)
- [with_old_parent_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_parent_folder_id.md)
- [hierarchy_move_or_copy](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/hierarchy_move_or_copy.md)
- [with_object_kind](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_object_kind.md)
- [change_cursor](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/change_cursor.md)
- [canonical_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical_folder_id.md)
- [canonical_message_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical_message_id.md)
- [change_kind](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/change_kind.md)
- [is_complete_for_wire](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/is_complete_for_wire.md)
- [source_hierarchy_table_event](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/source_hierarchy_table_event.md)
- [old_parent_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/old_parent_folder_id.md)
- [parent_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/parent_folder_id.md)
- [notification_total_messages](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/notification_total_messages.md)
- [notification_test_shape](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/notification_test_shape.md)
- [rop_register_notification_response](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_register_notification_response.md)
- [register_notification_success_response_matches_microsoft_wire_shape](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/register_notification_success_response_matches_microsoft_wire_shape.md)
- [new_mail_notification_with_message_id_encodes_exchange_zero_message_flags](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_with_message_id_encodes_exchange_zero_message_flags.md)
- [hierarchy_table_row_modified_notification_encodes_current_row](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_table_row_modified_notification_encodes_current_row.md)
- [new_mail_hierarchy_row_notification_encodes_message_row_keys](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_hierarchy_row_notification_encodes_message_row_keys.md)
- [folder_modified_notification_with_total_count_encodes_t_flag](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/folder_modified_notification_with_total_count_encodes_t_flag.md)
- [new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags.md)
- [object_moved_and_copied_notifications_preserve_source_message_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/object_moved_and_copied_notifications_preserve_source_message_id.md)
- [hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately.md)
- [hierarchy_move_emits_a_source_parent_table_refresh](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_move_emits_a_source_parent_table_refresh.md)
- [incomplete_message_move_notifications_are_not_serialized](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/incomplete_message_move_notifications_are_not_serialized.md)
- [incomplete_hierarchy_move_notification_is_not_serialized](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/incomplete_hierarchy_move_notification_is_not_serialized.md)
- [notification_wait_body](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_wait_body.md)
- [rop_notify_response](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response.md)
- [rop_hierarchy_table_row_modified_response](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_hierarchy_table_row_modified_response.md)
- [append_notification_data](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data.md)
- [append_event_object_ids](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_event_object_ids.md)
- [event_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/event_object_id.md)
- [append_wire_id](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_wire_id.md)
- [registration_matches_event](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/registration_matches_event.md)
- [notification_type_matches](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_type_matches.md)
- [notification_registration_from_request](../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_registration_from_request.md)

# Imports

- `super::identity::wire_id_bytes_from_object_id`
- `super::rop::*`
- `super::wire::{
    MapiNotificationEventMask, MAPI_CONTENT_NOTIFICATION_MASK, MAPI_HIERARCHY_NOTIFICATION_MASK,
}`
- `super::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)