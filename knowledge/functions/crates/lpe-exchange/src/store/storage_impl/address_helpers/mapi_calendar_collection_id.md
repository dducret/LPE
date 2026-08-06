---
type: Rust Function
title: mapi_calendar_collection_id
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L895-L908
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_identity_ids_from_row
---

# Signature

`fn mapi_calendar_collection_id( notification_account_id: Uuid, owner_account_id: Uuid, calendar_id: Uuid, calendar_role: &str, ) -> String`

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)
- [mapi_calendar_notification_event](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event.md)
- [mapi_calendar_notification_folder_identity_ids_from_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_identity_ids_from_row.md)