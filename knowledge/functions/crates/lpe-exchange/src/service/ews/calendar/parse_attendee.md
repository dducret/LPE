---
type: Rust Function
title: parse_attendee
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L607-L616
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_response_type_to_partstat
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
---

# Signature

`fn parse_attendee(attendee: &str, role: &str) -> Option<CalendarParticipantMetadata>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [ews_response_type_to_partstat](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_response_type_to_partstat.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)