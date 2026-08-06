---
type: Rust Function
title: mapi_calendar_notification_event
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L765-L817
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_collection_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_id
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_mask_for_change
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_canonical_ids
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_parent_folder_id
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_object_kind
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_create_update_delete_notifications_keep_stable_fid_mid
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_move_is_suppressed_without_a_distinct_old_message_id
---

# Signature

`fn mapi_calendar_notification_event( data: MapiCalendarNotificationData<'_>, calendar_folder_ids: &std::collections::HashMap<Uuid, u64>, ) -> Option<MapiNotificationEvent>`

# Calls

- [mapi_calendar_collection_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_collection_id.md)
- [mapi_calendar_notification_folder_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_id.md)
- [canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical.md)
- [mapi_notification_event_mask_for_change](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_mask_for_change.md)
- [with_canonical_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_canonical_ids.md)
- [with_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_parent_folder_id.md)
- [with_object_kind](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_object_kind.md)

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)
- [calendar_create_update_delete_notifications_keep_stable_fid_mid](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_create_update_delete_notifications_keep_stable_fid_mid.md)
- [calendar_move_is_suppressed_without_a_distinct_old_message_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/calendar_move_is_suppressed_without_a_distinct_old_message_id.md)