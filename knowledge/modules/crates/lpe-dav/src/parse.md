---
type: Rust Module
title: parse
resource: crates/lpe-dav/src/parse.rs#L1-L346
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-storage-calendar-attendee-labels-normalize-calendar-email-normalize-calendar-participation-status-serialize-calendar-participants-metadata-calendarorganizermetadata-calendarparticipantmetadata-calendarparticipantsmetadata-upsertclientcontactinput-upsertclienteventinput-upsertclienttaskinput
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-dav
---

# Contains

- [parse_vcard](../../../../functions/crates/lpe-dav/src/parse/parse_vcard.md)
- [parse_ical](../../../../functions/crates/lpe-dav/src/parse/parse_ical.md)
- [parse_vtodo](../../../../functions/crates/lpe-dav/src/parse/parse_vtodo.md)
- [parse_ical_datetime](../../../../functions/crates/lpe-dav/src/parse/parse_ical_datetime.md)
- [parse_ical_timestamp](../../../../functions/crates/lpe-dav/src/parse/parse_ical_timestamp.md)
- [parse_ical_duration](../../../../functions/crates/lpe-dav/src/parse/parse_ical_duration.md)
- [property_parameter](../../../../functions/crates/lpe-dav/src/parse/property_parameter.md)
- [parse_organizer](../../../../functions/crates/lpe-dav/src/parse/parse_organizer.md)
- [parse_attendee](../../../../functions/crates/lpe-dav/src/parse/parse_attendee.md)
- [task_status_from_vtodo_status](../../../../functions/crates/lpe-dav/src/parse/task_status_from_vtodo_status.md)
- [unfolded_lines](../../../../functions/crates/lpe-dav/src/parse/unfolded_lines.md)
- [text_unescape](../../../../functions/crates/lpe-dav/src/parse/text_unescape.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_storage::{
    calendar_attendee_labels, normalize_calendar_email, normalize_calendar_participation_status,
    serialize_calendar_participants_metadata, CalendarOrganizerMetadata,
    CalendarParticipantMetadata, CalendarParticipantsMetadata, UpsertClientContactInput,
    UpsertClientEventInput, UpsertClientTaskInput,
}`
- `uuid::Uuid`

# Member of

- [lpe-dav](../../../../packages/crates/lpe-dav.md)