---
type: Rust Function
title: event_end_minutes
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L350-L354
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_start_minutes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob
---

# Signature

`fn event_end_minutes(event: &AccessibleEvent) -> u32`

# Calls

- [event_start_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_start_minutes.md)

# Called by

- [calendar_recurrence_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob.md)