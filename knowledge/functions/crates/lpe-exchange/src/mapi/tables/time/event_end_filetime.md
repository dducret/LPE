---
type: Rust Function
title: event_end_filetime
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L11-L15
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/appointment_duration
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events
---

# Signature

`pub(in crate::mapi) fn event_end_filetime(event: &AccessibleEvent) -> u64`

# Calls

- [event_start_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
- [appointment_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/appointment_duration.md)
- [sort_events](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events.md)