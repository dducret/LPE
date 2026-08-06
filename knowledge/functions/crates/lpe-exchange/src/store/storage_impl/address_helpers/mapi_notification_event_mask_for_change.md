---
type: Rust Function
title: mapi_notification_event_mask_for_change
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L1016-L1025
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event
---

# Signature

`fn mapi_notification_event_mask_for_change(change_kind: &str, is_new_mail: bool) -> u16`

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)
- [mapi_calendar_notification_event](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event.md)