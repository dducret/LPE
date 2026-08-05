---
type: Rust Function
title: mapi_calendar_notification_folder_id
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L717-L729
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id_for_collection
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event
---

# Signature

`fn mapi_calendar_notification_folder_id( collection_id: &str, calendar_folder_ids: &std::collections::HashMap<Uuid, u64>, ) -> Option<u64>`

# Calls

- [collaboration_folder_identity_canonical_id_for_collection](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [mapi_collaboration_folder_id_for_collection](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id_for_collection.md)

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)
- [mapi_calendar_notification_event](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event.md)