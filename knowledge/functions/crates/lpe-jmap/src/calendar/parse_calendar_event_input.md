---
type: Rust Function
title: parse_calendar_event_input
resource: crates/lpe-jmap/src/calendar.rs#L1133-L1216
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/calendar/reject_unknown_calendar_event_properties
  - functions/crates/lpe-jmap/src/calendar/validate_calendar_ids
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/parse/parse_local_datetime
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_attachment_inputs
  - functions/crates/lpe-jmap/src/parse/parse_optional_string
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_duration
  - functions/crates/lpe-jmap/src/parse/parse_required_string
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_location
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_participants
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_participants_json
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input
---

# Signature

`fn parse_calendar_event_input( id: Option<Uuid>, account_id: Uuid, value: Value, ) -> Result<( Option<String>, UpsertClientEventInput, Vec<CalendarAttachmentInput>, )>`

# Calls

- [reject_unknown_calendar_event_properties](../../../../../functions/crates/lpe-jmap/src/calendar/reject_unknown_calendar_event_properties.md)
- [validate_calendar_ids](../../../../../functions/crates/lpe-jmap/src/calendar/validate_calendar_ids.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_local_datetime](../../../../../functions/crates/lpe-jmap/src/parse/parse_local_datetime.md)
- [parse_calendar_attachment_inputs](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_attachment_inputs.md)
- [parse_optional_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_optional_string.md)
- [parse_calendar_duration](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_duration.md)
- [parse_required_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_required_string.md)
- [parse_calendar_location](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_location.md)
- [parse_calendar_participants](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_participants.md)
- [parse_calendar_participants_json](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_participants_json.md)

# Called by

- [handle_calendar_event_set](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set.md)
- [calendar_event_update_input](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input.md)