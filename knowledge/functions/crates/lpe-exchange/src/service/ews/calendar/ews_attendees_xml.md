---
type: Rust Function
title: ews_attendees_xml
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L265-L282
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_attendee_collection_xml
---

# Signature

`fn ews_attendees_xml(event: &AccessibleEvent) -> String`

# Calls

- [parse_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [ews_attendee_collection_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_attendee_collection_xml.md)