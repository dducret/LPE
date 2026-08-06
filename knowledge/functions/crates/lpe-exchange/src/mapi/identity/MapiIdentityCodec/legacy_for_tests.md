---
type: Rust Method
title: legacy_for_tests
resource: crates/lpe-exchange/src/mapi/identity.rs#L167-L180
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/logical_special_folder_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate
  - functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_with_message_id_encodes_exchange_zero_message_flags
  - functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_table_row_modified_notification_encodes_current_row
  - functions/crates/lpe-exchange/src/mapi/notifications/folder_modified_notification_with_total_count_encodes_t_flag
  - functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags
  - functions/crates/lpe-exchange/src/mapi/notifications/object_moved_and_copied_notifications_preserve_source_message_id
  - functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately
  - functions/crates/lpe-exchange/src/mapi/notifications/incomplete_message_move_notifications_are_not_serialized
  - functions/crates/lpe-exchange/src/mapi/notifications/incomplete_hierarchy_move_notification_is_not_serialized
  - functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_derives_counted_folder_modified_notification_for_collaboration_content_create
  - functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_contents_property_row_uses_durable_message_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/default_calendar_uses_reserved_fid_without_an_identity_record
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/custom_calendar_fails_closed_without_a_principal_scoped_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/scoped_snapshot_retains_all_durable_identity_records
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
---

# Signature

`pub(crate) fn legacy_for_tests() -> Self`

# Calls

- [logical_special_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/logical_special_folder_ids.md)

# Called by

- [owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate.md)
- [new_mail_notification_with_message_id_encodes_exchange_zero_message_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_with_message_id_encodes_exchange_zero_message_flags.md)
- [hierarchy_table_row_modified_notification_encodes_current_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_table_row_modified_notification_encodes_current_row.md)
- [folder_modified_notification_with_total_count_encodes_t_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/folder_modified_notification_with_total_count_encodes_t_flag.md)
- [new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_notification_without_message_class_defaults_to_ipm_note_and_zero_message_flags.md)
- [object_moved_and_copied_notifications_preserve_source_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/object_moved_and_copied_notifications_preserve_source_message_id.md)
- [hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_moved_and_copied_notifications_encode_old_folder_and_parent_separately.md)
- [incomplete_message_move_notifications_are_not_serialized](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/incomplete_message_move_notifications_are_not_serialized.md)
- [incomplete_hierarchy_move_notification_is_not_serialized](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/incomplete_hierarchy_move_notification_is_not_serialized.md)
- [saved_message_handle_getprops_keeps_batch_email_and_durable_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity.md)
- [session_derives_counted_folder_modified_notification_for_collaboration_content_create](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_derives_counted_folder_modified_notification_for_collaboration_content_create.md)
- [notification_subscription_preserves_rop_logon_id_through_rop_notify](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify.md)
- [normal_contents_property_row_uses_durable_message_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_contents_property_row_uses_durable_message_identity.md)
- [default_calendar_uses_reserved_fid_without_an_identity_record](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/default_calendar_uses_reserved_fid_without_an_identity_record.md)
- [custom_calendar_fails_closed_without_a_principal_scoped_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/custom_calendar_fails_closed_without_a_principal_scoped_identity.md)
- [scoped_snapshot_retains_all_durable_identity_records](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/scoped_snapshot_retains_all_durable_identity_records.md)
- [build](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)