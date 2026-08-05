---
type: Rust Function
title: mapi_folder_id_from_role_or_identity
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L805-L809
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_folder_id
---

# Signature

`fn mapi_folder_id_from_role_or_identity(role: Option<&str>, identity: Option<i64>) -> Option<u64>`

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)
- [mapi_notification_folder_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_folder_id.md)