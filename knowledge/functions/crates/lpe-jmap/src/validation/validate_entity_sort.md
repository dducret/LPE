---
type: Rust Function
title: validate_entity_sort
resource: crates/lpe-jmap/src/validation.rs#L20-L35
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes
---

# Signature

`pub(crate) fn validate_entity_sort( sort: Option<&[EntityQuerySort]>, expected_property: &str, ascending: bool, ) -> Result<()>`

# Called by

- [handle_calendar_event_query](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query.md)
- [handle_calendar_event_query_changes](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes.md)
- [handle_contact_query](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query.md)
- [handle_contact_query_changes](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes.md)