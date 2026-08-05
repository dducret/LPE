---
type: Rust Function
title: has_jmap_property_patch
resource: crates/lpe-jmap/src/convert.rs#L40-L42
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input
  - functions/crates/lpe-jmap/src/contacts/JmapService/contact_update_input
---

# Signature

`pub(crate) fn has_jmap_property_patch(object: &Map<String, Value>) -> bool`

# Called by

- [calendar_update_name](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name.md)
- [calendar_event_update_input](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input.md)
- [contact_update_input](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/contact_update_input.md)