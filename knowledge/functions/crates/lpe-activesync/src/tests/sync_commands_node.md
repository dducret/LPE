---
type: Rust Function
title: sync_commands_node
resource: crates/lpe-activesync/src/tests.rs#L6081-L6095
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields
  - functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees
---

# Signature

`fn sync_commands_node(collection_id: &str, sync_key: &str, commands: Vec<WbxmlNode>) -> WbxmlNode`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [sync_contact_create_update_delete_round_trips_canonical_fields](../../../../../functions/crates/lpe-activesync/src/tests/sync_contact_create_update_delete_round_trips_canonical_fields.md)
- [sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees](../../../../../functions/crates/lpe-activesync/src/tests/sync_calendar_create_update_delete_maps_time_zone_recurrence_and_attendees.md)