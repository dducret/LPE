---
type: Rust Function
title: appointment_duration
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L256-L263
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/time/event_end_filetime
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
---

# Signature

`fn appointment_duration(event: &AccessibleEvent) -> i32`

# Calls

- [event_start_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime.md)
- [event_end_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_end_filetime.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)