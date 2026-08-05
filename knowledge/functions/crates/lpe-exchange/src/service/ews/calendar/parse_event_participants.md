---
type: Rust Function
title: parse_event_participants
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L581-L608
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
---

# Signature

`fn parse_event_participants( principal: &AccountPrincipal, event: &str, ) -> (CalendarParticipantsMetadata, bool)`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)

# Called by

- [parse_create_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input.md)
- [parse_update_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)