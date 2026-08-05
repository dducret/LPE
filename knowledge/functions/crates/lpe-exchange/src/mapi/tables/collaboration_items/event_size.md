---
type: Rust Function
title: event_size
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L14-L21
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  - functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object
---

# Signature

`pub(in crate::mapi) fn event_size(event: &AccessibleEvent) -> i64`

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
- [calendar_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object.md)