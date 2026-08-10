---
type: Rust Function
title: calendar_global_object_id
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L491-L518
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_fallback_global_object_id_uses_zero_creation_time
---

# Signature

`pub(super) fn calendar_global_object_id(event: &AccessibleEvent) -> Vec<u8>`

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
- [calendar_fallback_global_object_id_uses_zero_creation_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_fallback_global_object_id_uses_zero_creation_time.md)