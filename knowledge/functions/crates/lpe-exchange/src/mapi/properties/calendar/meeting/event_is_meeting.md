---
type: Rust Function
title: event_is_meeting
resource: crates/lpe-exchange/src/mapi/properties/calendar/meeting.rs#L62-L77
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/appointment_state_flags
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/organizer_json_from_mapi
---

# Signature

`fn event_is_meeting(event: &AccessibleEvent) -> bool`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_calendar_participants_metadata](../../../../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)

# Called by

- [appointment_state_flags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/appointment_state_flags.md)
- [organizer_json_from_mapi](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/organizer_json_from_mapi.md)