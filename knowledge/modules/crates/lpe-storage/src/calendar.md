---
type: Rust Module
title: calendar
resource: crates/lpe-storage/src/calendar.rs#L1-L220
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/lpe-domain-normalization
  - external/serde-deserialize-serialize
  - external/serde-json-value
  - external/super
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [CalendarParticipantMetadata](../../../../classes/crates/lpe-storage/src/calendar/CalendarParticipantMetadata.md)
- [CalendarOrganizerMetadata](../../../../classes/crates/lpe-storage/src/calendar/CalendarOrganizerMetadata.md)
- [CalendarParticipantsMetadata](../../../../classes/crates/lpe-storage/src/calendar/CalendarParticipantsMetadata.md)
- [parse_calendar_participants_metadata](../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [serialize_calendar_participants_metadata](../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)
- [calendar_attendee_labels](../../../../functions/crates/lpe-storage/src/calendar/calendar_attendee_labels.md)
- [calendar_participant_label](../../../../functions/crates/lpe-storage/src/calendar/calendar_participant_label.md)
- [normalize_calendar_email](../../../../functions/crates/lpe-storage/src/calendar/normalize_calendar_email.md)
- [normalize_calendar_participation_status](../../../../functions/crates/lpe-storage/src/calendar/normalize_calendar_participation_status.md)
- [normalize_calendar_participants_metadata](../../../../functions/crates/lpe-storage/src/calendar/normalize_calendar_participants_metadata.md)
- [serialized_calendar_participants_match_attendees_json_schema](../../../../functions/crates/lpe-storage/src/calendar/serialized_calendar_participants_match_attendees_json_schema.md)
- [parser_still_accepts_legacy_combined_participant_metadata](../../../../functions/crates/lpe-storage/src/calendar/parser_still_accepts_legacy_combined_participant_metadata.md)

# Imports

- `lpe_domain::normalization`
- `serde::{Deserialize, Serialize}`
- `serde_json::Value`
- `super::*`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)