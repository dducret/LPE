---
type: Rust Function
title: mapi_calendar_notification_folder_identity_ids_from_row
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L792-L849
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_collection_id
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/push_unique_uuid
---

# Signature

`fn mapi_calendar_notification_folder_identity_ids_from_row( row: &sqlx::postgres::PgRow, ) -> Vec<Uuid>`

# Calls

- [mapi_calendar_collection_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_collection_id.md)
- [collaboration_folder_identity_canonical_id_for_collection](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection.md)
- [push_unique_uuid](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/push_unique_uuid.md)