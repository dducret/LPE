---
type: Rust Function
title: mapi_notification_event_from_change_row
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L144-L635
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_mask_for_change
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_folder_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_hierarchy_old_parent_folder_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_hierarchy_movement_source_ids
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_canonical_ids
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_parent_folder_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_event_object_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_contact_notification_folder_id
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_object_kind
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_collection_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_id
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_message_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_folder_id_from_role_or_identity
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_message_object_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_old_message_id
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_parent_folder_id
---

# Signature

`fn mapi_notification_event_from_change_row( row: sqlx::postgres::PgRow, calendar_folder_ids: &std::collections::HashMap<Uuid, u64>, calendar_event_ids: &std::collections::HashMap<Uuid, u64>, contact_ids: &std::collections::HashMap<Uuid, u64>, mailbox_folder_ids: &std::collections::HashMap<Uuid, u64>, mailbox_message_ids: &std::collections::HashMap<Uuid, u64>, ) -> Option<MapiNotificationEvent>`

# Calls

- [mapi_notification_event_mask_for_change](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_mask_for_change.md)
- [mapi_notification_folder_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_folder_id.md)
- [virtual_special_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata.md)
- [mapi_hierarchy_old_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_hierarchy_old_parent_folder_id.md)
- [mapi_hierarchy_movement_source_ids](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_hierarchy_movement_source_ids.md)
- [canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical.md)
- [with_canonical_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_canonical_ids.md)
- [with_old_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_parent_folder_id.md)
- [mapi_calendar_event_object_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_event_object_id.md)
- [mapi_calendar_notification_event](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mapi_contact_notification_folder_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_contact_notification_folder_id.md)
- [with_object_kind](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_object_kind.md)
- [mapi_calendar_collection_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_collection_id.md)
- [mapi_calendar_notification_folder_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_id.md)
- [with_old_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_old_message_id.md)
- [mapi_folder_id_from_role_or_identity](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_folder_id_from_role_or_identity.md)
- [mapi_notification_message_object_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_message_object_id.md)
- [mapi_notification_old_message_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_old_message_id.md)
- [with_parent_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/with_parent_folder_id.md)