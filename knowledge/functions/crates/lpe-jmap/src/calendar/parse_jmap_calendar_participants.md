---
type: Rust Function
title: parse_jmap_calendar_participants
resource: crates/lpe-jmap/src/calendar.rs#L1315-L1374
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/calendar/participant_email
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_participants
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_participants_json
---

# Signature

`fn parse_jmap_calendar_participants(value: Option<&Value>) -> Result<CalendarParticipantsMetadata>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [participant_email](../../../../../functions/crates/lpe-jmap/src/calendar/participant_email.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_calendar_participants](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_participants.md)
- [parse_calendar_participants_json](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_participants_json.md)