---
type: Rust Function
title: calendar_to_value
resource: crates/lpe-jmap/src/calendar.rs#L754-L781
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_get
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name
---

# Signature

`fn calendar_to_value(collection: &CollaborationCollection, properties: &HashSet<String>) -> Value`

# Calls

- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_calendar_get](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_get.md)
- [calendar_update_name](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name.md)