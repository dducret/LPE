---
type: Rust Function
title: calendar_time_zone_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L853-L875
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition_key
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/canonical_calendar_time_zone_key
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
---

# Signature

`fn calendar_time_zone_from_mapi(properties: &HashMap<u32, MapiValue>) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [calendar_time_zone_definition_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition_key.md)
- [canonical_calendar_time_zone_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/canonical_calendar_time_zone_key.md)
- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)

# Called by

- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)