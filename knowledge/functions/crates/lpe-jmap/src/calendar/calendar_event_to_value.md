---
type: Rust Function
title: calendar_event_to_value
resource: crates/lpe-jmap/src/calendar.rs#L814-L918
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  - functions/crates/lpe-jmap/src/calendar/insert_json_if
  - functions/crates/lpe-jmap/src/calendar/participants_from_event
  - functions/crates/lpe-jmap/src/calendar/calendar_attachment_links
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input
---

# Signature

`fn calendar_event_to_value( event: &AccessibleEvent, properties: &HashSet<String>, attachments: &[CalendarEventAttachment], ) -> Value`

# Calls

- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)
- [insert_json_if](../../../../../functions/crates/lpe-jmap/src/calendar/insert_json_if.md)
- [participants_from_event](../../../../../functions/crates/lpe-jmap/src/calendar/participants_from_event.md)
- [calendar_attachment_links](../../../../../functions/crates/lpe-jmap/src/calendar/calendar_attachment_links.md)

# Called by

- [handle_calendar_event_get](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get.md)
- [calendar_event_update_input](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input.md)