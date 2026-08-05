---
type: Rust Method
title: with_object_kind
resource: crates/lpe-exchange/src/mapi/notifications.rs#L207-L210
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event
---

# Signature

`pub(crate) fn with_object_kind(mut self, object_kind: &'static str) -> Self`

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)
- [mapi_calendar_notification_event](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_event.md)