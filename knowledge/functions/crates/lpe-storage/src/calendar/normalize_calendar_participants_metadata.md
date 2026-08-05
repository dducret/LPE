---
type: Rust Function
title: normalize_calendar_participants_metadata
resource: crates/lpe-storage/src/calendar.rs#L139-L174
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
---

# Signature

`fn normalize_calendar_participants_metadata( mut metadata: CalendarParticipantsMetadata, ) -> CalendarParticipantsMetadata`

# Called by

- [parse_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [serialize_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)