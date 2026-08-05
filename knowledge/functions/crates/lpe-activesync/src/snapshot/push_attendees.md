---
type: Rust Function
title: push_attendees
resource: crates/lpe-activesync/src/snapshot.rs#L287-L316
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/push_text
  - functions/crates/lpe-activesync/src/snapshot/attendee_status
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/calendar_application_data
---

# Signature

`fn push_attendees(children: &mut Vec<Value>, attendees: &[CalendarParticipantMetadata])`

# Calls

- [push_text](../../../../../functions/crates/lpe-activesync/src/snapshot/push_text.md)
- [attendee_status](../../../../../functions/crates/lpe-activesync/src/snapshot/attendee_status.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [calendar_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/calendar_application_data.md)