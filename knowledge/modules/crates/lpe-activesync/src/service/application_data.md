---
type: Rust Module
title: application_data
resource: crates/lpe-activesync/src/service/application_data.rs#L1-L509
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-days-from-civil
  - external/lpe-storage-calendar-attendee-labels-serialize-calendar-participants-metadata-calendarparticipantmetadata-calendarparticipantsmetadata-contactnamefields-jmapemailfollowupupdate-upsertclientcontactinput-upsertclienteventinput
  - external/uuid-uuid
  - external/crate-message-field-text-wbxml-wbxmlnode
  - external/super-parse-contact-input
  - external/crate-wbxml-wbxmlnode
  - external/lpe-storage-clientcontact-contactsourcefields
  - external/serde-json-json
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [mail_flag_update](../../../../../functions/crates/lpe-activesync/src/service/application_data/mail_flag_update.md)
- [active_sync_datetime_to_rfc3339](../../../../../functions/crates/lpe-activesync/src/service/application_data/active_sync_datetime_to_rfc3339.md)
- [parse_contact_input](../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_contact_input.md)
- [parse_event_input](../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_event_input.md)
- [body_text](../../../../../functions/crates/lpe-activesync/src/service/application_data/body_text.md)
- [parse_compact_datetime](../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_compact_datetime.md)
- [duration_from_datetimes](../../../../../functions/crates/lpe-activesync/src/service/application_data/duration_from_datetimes.md)
- [date_time_to_minutes](../../../../../functions/crates/lpe-activesync/src/service/application_data/date_time_to_minutes.md)
- [attendees_from_nodes](../../../../../functions/crates/lpe-activesync/src/service/application_data/attendees_from_nodes.md)
- [recurrence_to_rrule](../../../../../functions/crates/lpe-activesync/src/service/application_data/recurrence_to_rrule.md)
- [day_of_week_to_rrule](../../../../../functions/crates/lpe-activesync/src/service/application_data/day_of_week_to_rrule.md)
- [parse_positive_number](../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_positive_number.md)
- [compact_datetime_date](../../../../../functions/crates/lpe-activesync/src/service/application_data/compact_datetime_date.md)
- [activesync_contact_narrow_update_omits_unowned_rich_fields](../../../../../functions/crates/lpe-activesync/src/service/application_data/activesync_contact_narrow_update_omits_unowned_rich_fields.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::days_from_civil`
- `lpe_storage::{
    calendar_attendee_labels, serialize_calendar_participants_metadata,
    CalendarParticipantMetadata, CalendarParticipantsMetadata, ContactNameFields,
    JmapEmailFollowupUpdate, UpsertClientContactInput, UpsertClientEventInput,
}`
- `uuid::Uuid`
- `crate::{message::field_text, wbxml::WbxmlNode}`
- `super::parse_contact_input`
- `crate::wbxml::WbxmlNode`
- `lpe_storage::{ClientContact, ContactSourceFields}`
- `serde_json::json`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)