---
type: Rust Function
title: calendar_application_data
resource: crates/lpe-activesync/src/snapshot.rs#L228-L263
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/push_text
  - functions/crates/lpe-activesync/src/snapshot/compact_datetime
  - functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-activesync/src/snapshot/push_attendees
  - functions/crates/lpe-activesync/src/snapshot/push_body
  - functions/crates/lpe-activesync/src/snapshot/recurrence_application_data
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes
---

# Signature

`pub(crate) fn calendar_application_data(event: &ClientEvent) -> Value`

# Calls

- [push_text](../../../../../functions/crates/lpe-activesync/src/snapshot/push_text.md)
- [compact_datetime](../../../../../functions/crates/lpe-activesync/src/snapshot/compact_datetime.md)
- [add_minutes_to_compact](../../../../../functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact.md)
- [parse_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [push_attendees](../../../../../functions/crates/lpe-activesync/src/snapshot/push_attendees.md)
- [push_body](../../../../../functions/crates/lpe-activesync/src/snapshot/push_body.md)
- [recurrence_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/recurrence_application_data.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [fetch_collection_nodes](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes.md)