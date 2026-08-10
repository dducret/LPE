---
type: Rust Function
title: reject_unsupported_mapi_event_properties
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L1124-L1152
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_calendar_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_opaque_binary_properties_are_accepted
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_state_flags_map_bounded_cancel_state
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_meeting_classes_fail_explicitly
---

# Signature

`pub(in crate::mapi) fn reject_unsupported_mapi_event_properties( properties: &HashMap<u32, MapiValue>, ) -> Result<()>`

# Calls

- [reject_unsupported_calendar_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_calendar_message_class.md)
- [as_i64](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)

# Called by

- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)
- [mapi_over_http_calendar_opaque_binary_properties_are_accepted](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_opaque_binary_properties_are_accepted.md)
- [mapi_over_http_calendar_state_flags_map_bounded_cancel_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_state_flags_map_bounded_cancel_state.md)
- [mapi_over_http_calendar_meeting_classes_fail_explicitly](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_meeting_classes_fail_explicitly.md)