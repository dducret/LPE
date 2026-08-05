---
type: Rust Function
title: push_unique_uuid
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L1201-L1205
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_identity_ids_from_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_mailbox_notification_identity_ids_from_row
---

# Signature

`fn push_unique_uuid(values: &mut Vec<Uuid>, value: Uuid)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_calendar_notification_folder_identity_ids_from_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_identity_ids_from_row.md)
- [mapi_mailbox_notification_identity_ids_from_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_mailbox_notification_identity_ids_from_row.md)