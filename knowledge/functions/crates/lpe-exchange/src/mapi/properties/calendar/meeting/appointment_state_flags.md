---
type: Rust Function
title: appointment_state_flags
resource: crates/lpe-exchange/src/mapi/properties/calendar/meeting.rs#L5-L14
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/event_is_meeting
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/response_status
---

# Signature

`pub(super) fn appointment_state_flags(event: &AccessibleEvent) -> i32`

# Calls

- [event_is_meeting](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/event_is_meeting.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
- [response_status](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/response_status.md)