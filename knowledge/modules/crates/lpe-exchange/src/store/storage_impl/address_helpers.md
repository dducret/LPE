---
type: Rust Module
title: address_helpers
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L1-L1522
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-mapi-calendar-event-object-id-mapi-calendar-notification-event-mapi-hierarchy-movement-source-ids-mapi-hierarchy-old-parent-folder-id-mapi-notification-event-mask-for-change-mapi-notification-message-object-id-mapi-notification-old-message-id-mapicalendarnotificationdata
  - external/crate-mapi-notifications-mapinotificationkind
  - external/std-collections-hashmap
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [task_matches_collection](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/task_matches_collection.md)
- [directory_kind_from_storage](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/directory_kind_from_storage.md)
- [address_book_details_from_contact](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/address_book_details_from_contact.md)
- [contact_phone_by_label](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/contact_phone_by_label.md)
- [contact_phone_values_by_label](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/contact_phone_values_by_label.md)
- [contact_labeled_json_values](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/contact_labeled_json_values.md)
- [contact_address_value](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/contact_address_value.md)
- [address_book_group_display_name](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/address_book_group_display_name.md)
- [mapi_tenant_id_for_account](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_tenant_id_for_account.md)
- [mapi_identity_lookup_from_row](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_identity_lookup_from_row.md)
- [mapi_notification_event_from_change_row](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)
- [mapi_notification_message_object_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_message_object_id.md)
- [mapi_notification_old_message_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_old_message_id.md)
- [mapi_hierarchy_movement_source_ids](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_hierarchy_movement_source_ids.md)
- [mapi_calendar_event_object_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_event_object_id.md)
- [MapiCalendarNotificationData](../../../../../../classes/crates/lpe-exchange/src/store/storage_impl/address_helpers/MapiCalendarNotificationData.md)
- [mapi_calendar_notification_event](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event.md)
- [mapi_calendar_notification_folder_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_id.md)
- [mapi_calendar_notification_folder_identity_ids_from_row](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_identity_ids_from_row.md)
- [mapi_calendar_collection_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_collection_id.md)
- [mapi_folder_id_from_role_or_identity](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_folder_id_from_role_or_identity.md)
- [mapi_notification_folder_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_folder_id.md)
- [mapi_hierarchy_old_parent_folder_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_hierarchy_old_parent_folder_id.md)
- [mapi_mailbox_notification_identity_ids_from_row](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_mailbox_notification_identity_ids_from_row.md)
- [mapi_notification_event_mask_for_change](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_mask_for_change.md)
- [inbox_delivery_uses_new_mail_notification_mask](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/inbox_delivery_uses_new_mail_notification_mask.md)
- [hierarchy_movement_uses_strict_source_folder_and_parent_ids](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/hierarchy_movement_uses_strict_source_folder_and_parent_ids.md)
- [hierarchy_old_parent_requires_explicit_metadata_but_preserves_root](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/hierarchy_old_parent_requires_explicit_metadata_but_preserves_root.md)
- [mailbox_move_notification_requires_durable_message_identity_snapshot](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mailbox_move_notification_requires_durable_message_identity_snapshot.md)
- [mailbox_move_notification_uses_historical_destination_message_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mailbox_move_notification_uses_historical_destination_message_id.md)
- [message_notification_without_durable_mid_is_not_publishable](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/message_notification_without_durable_mid_is_not_publishable.md)
- [calendar_create_update_delete_notifications_keep_stable_fid_mid](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_create_update_delete_notifications_keep_stable_fid_mid.md)
- [calendar_move_is_suppressed_without_a_distinct_old_message_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_move_is_suppressed_without_a_distinct_old_message_id.md)
- [calendar_notification_identity_never_falls_back_to_another_principal_cache_entry](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_notification_identity_never_falls_back_to_another_principal_cache_entry.md)
- [mapi_sync_checkpoint_from_row](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_sync_checkpoint_from_row.md)
- [push_unique_uuid](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/push_unique_uuid.md)
- [push_unique_associated_config_change](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/push_unique_associated_config_change.md)
- [ews_mail_app_catalog_id](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/ews_mail_app_catalog_id.md)
- [ews_update_mail_app_install_status](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/ews_update_mail_app_install_status.md)
- [validate_ews_im_member_in_tx](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/validate_ews_im_member_in_tx.md)
- [insert_ews_im_member_in_tx](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/insert_ews_im_member_in_tx.md)
- [ews_unified_messaging_call_select_sql](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/ews_unified_messaging_call_select_sql.md)
- [ews_unified_messaging_call_from_row](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/ews_unified_messaging_call_from_row.md)

# Imports

- `super::{
        mapi_calendar_event_object_id, mapi_calendar_notification_event,
        mapi_hierarchy_movement_source_ids, mapi_hierarchy_old_parent_folder_id,
        mapi_notification_event_mask_for_change,
        mapi_notification_message_object_id, mapi_notification_old_message_id,
        MapiCalendarNotificationData,
    }`
- `crate::mapi::notifications::MapiNotificationKind`
- `std::collections::HashMap`
- `uuid::Uuid`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)