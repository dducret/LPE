---
type: Rust Function
title: calendar_body_text_for_mapi
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L262-L268
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  - functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object
---

# Signature

`pub(in crate::mapi) fn calendar_body_text_for_mapi(event: &AccessibleEvent) -> String`

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
- [calendar_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/calendar_sync_object.md)