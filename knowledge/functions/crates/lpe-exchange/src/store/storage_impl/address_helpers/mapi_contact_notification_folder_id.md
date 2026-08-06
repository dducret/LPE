---
type: Rust Function
title: mapi_contact_notification_folder_id
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L672-L687
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
---

# Signature

`fn mapi_contact_notification_folder_id( notification_account_id: Uuid, owner_account_id: Uuid, contact_book_role: &str, ) -> Option<u64>`

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)