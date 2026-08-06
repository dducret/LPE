---
type: Rust Function
title: reject_unsupported_calendar_message_class
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L1026-L1042
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_mapi_event_properties
---

# Signature

`fn reject_unsupported_calendar_message_class(properties: &HashMap<u32, MapiValue>) -> Result<()>`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)

# Called by

- [reject_unsupported_mapi_event_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_mapi_event_properties.md)