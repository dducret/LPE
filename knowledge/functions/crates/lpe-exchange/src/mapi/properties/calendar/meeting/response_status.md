---
type: Rust Function
title: response_status
resource: crates/lpe-exchange/src/mapi/properties/calendar/meeting.rs#L16-L22
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/appointment_state_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
---

# Signature

`pub(super) fn response_status(event: &AccessibleEvent) -> i32`

# Calls

- [appointment_state_flags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/appointment_state_flags.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)