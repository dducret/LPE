---
type: Rust Function
title: event_start_minutes
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L346-L348
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/time_to_minutes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_end_minutes
---

# Signature

`fn event_start_minutes(event: &AccessibleEvent) -> u32`

# Calls

- [time_to_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/time_to_minutes.md)

# Called by

- [calendar_recurrence_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob.md)
- [event_end_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/event_end_minutes.md)