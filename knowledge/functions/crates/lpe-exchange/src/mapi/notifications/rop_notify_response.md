---
type: Rust Function
title: rop_notify_response
resource: crates/lpe-exchange/src/mapi/notifications.rs#L640-L654
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/is_complete_for_wire
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_maps_logical_default_folder_ids_to_durable_ids
  - functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_with_message_id_encodes_exchange_zero_message_flags
  - functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags
  - functions/crates/lpe-exchange/src/mapi/notifications/object_moved_and_copied_notifications_preserve_source_message_id
  - functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately
  - functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify
---

# Signature

`pub(in crate::mapi) fn rop_notify_response( identity_codec: &crate::mapi::identity::MapiIdentityCodec, notification_handle: u32, logon_id: u8, event: &MapiNotificationEvent, ) -> Option<Vec<u8>>`

# Calls

- [is_complete_for_wire](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/is_complete_for_wire.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [append_notification_data](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data.md)

# Called by

- [execute_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [scoped_codec_maps_logical_default_folder_ids_to_durable_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_maps_logical_default_folder_ids_to_durable_ids.md)
- [new_mail_notification_with_message_id_encodes_exchange_zero_message_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_with_message_id_encodes_exchange_zero_message_flags.md)
- [new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags.md)
- [object_moved_and_copied_notifications_preserve_source_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/object_moved_and_copied_notifications_preserve_source_message_id.md)
- [hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately.md)
- [notification_subscription_preserves_rop_logon_id_through_rop_notify](../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify.md)