---
type: Rust Method
title: with_old_parent_folder_id
resource: crates/lpe-exchange/src/mapi/notifications.rs#L160-L163
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
---

# Signature

`pub(crate) fn with_old_parent_folder_id(mut self, old_parent_folder_id: Option<u64>) -> Self`

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)