---
type: Rust Function
title: parse_event_input
resource: crates/lpe-activesync/src/service/application_data.rs#L152-L245
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/application_data/parse_compact_datetime
  - functions/crates/lpe-activesync/src/service/application_data/duration_from_datetimes
  - functions/crates/lpe-activesync/src/service/application_data/attendees_from_nodes
  - functions/crates/lpe-activesync/src/service/application_data/body_text
  - functions/crates/lpe-activesync/src/service/application_data/recurrence_to_rrule
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_calendar_sync_commands
---

# Signature

`pub(super) fn parse_event_input( account_id: Uuid, id: Option<Uuid>, existing: Option<&lpe_storage::ClientEvent>, application_data: &WbxmlNode, ) -> Result<UpsertClientEventInput>`

# Calls

- [parse_compact_datetime](../../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_compact_datetime.md)
- [duration_from_datetimes](../../../../../../functions/crates/lpe-activesync/src/service/application_data/duration_from_datetimes.md)
- [attendees_from_nodes](../../../../../../functions/crates/lpe-activesync/src/service/application_data/attendees_from_nodes.md)
- [body_text](../../../../../../functions/crates/lpe-activesync/src/service/application_data/body_text.md)
- [recurrence_to_rrule](../../../../../../functions/crates/lpe-activesync/src/service/application_data/recurrence_to_rrule.md)

# Called by

- [apply_calendar_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_calendar_sync_commands.md)