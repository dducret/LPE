---
type: Rust Function
title: serialize_ical
resource: crates/lpe-dav/src/serialize.rs#L22-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/serialize/format_ical_datetime
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-dav/src/serialize/push_line
  - functions/crates/lpe-dav/src/serialize/push_raw_line
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-dav/src/serialize/serialize_organizer
  - functions/crates/lpe-dav/src/serialize/serialize_attendee
  called_by:
  - functions/crates/lpe-dav/src/paths/etag_for_event
  - functions/crates/lpe-dav/src/propfind/event_resource_entry
  - functions/crates/lpe-dav/src/propfind/event_report_entry
  - functions/crates/lpe-dav/src/service/DavService/handle_get
---

# Signature

`pub(crate) fn serialize_ical(event: &AccessibleEvent) -> String`

# Calls

- [format_ical_datetime](../../../../../functions/crates/lpe-dav/src/serialize/format_ical_datetime.md)
- [parse_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [push_line](../../../../../functions/crates/lpe-dav/src/serialize/push_line.md)
- [push_raw_line](../../../../../functions/crates/lpe-dav/src/serialize/push_raw_line.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [serialize_organizer](../../../../../functions/crates/lpe-dav/src/serialize/serialize_organizer.md)
- [serialize_attendee](../../../../../functions/crates/lpe-dav/src/serialize/serialize_attendee.md)

# Called by

- [etag_for_event](../../../../../functions/crates/lpe-dav/src/paths/etag_for_event.md)
- [event_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/event_resource_entry.md)
- [event_report_entry](../../../../../functions/crates/lpe-dav/src/propfind/event_report_entry.md)
- [handle_get](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)