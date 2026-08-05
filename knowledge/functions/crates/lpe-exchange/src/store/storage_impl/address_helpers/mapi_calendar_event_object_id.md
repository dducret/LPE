---
type: Rust Function
title: mapi_calendar_event_object_id
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L636-L644
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
---

# Signature

`fn mapi_calendar_event_object_id( durable_object_id: Option<i64>, event_id: Option<Uuid>, scoped_event_ids: &std::collections::HashMap<Uuid, u64>, ) -> Option<u64>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)