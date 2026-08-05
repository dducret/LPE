---
type: Rust Function
title: ews_attendee_collection_xml
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L287-L297
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_attendees_xml
---

# Signature

`fn ews_attendee_collection_xml<'a>( element_name: &str, attendees: impl Iterator<Item = &'a CalendarParticipantMetadata>, ) -> String`

# Called by

- [ews_attendees_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_attendees_xml.md)