---
type: Rust Function
title: event_start_filetime
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L7-L9
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/appointment_duration
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_reminder_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/time/event_end_filetime
---

# Signature

`pub(in crate::mapi) fn event_start_filetime(event: &AccessibleEvent) -> u64`

# Calls

- [date_time_to_filetime_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
- [appointment_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/appointment_duration.md)
- [event_reminder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_reminder_property_value.md)
- [event_end_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_end_filetime.md)