---
type: Rust Function
title: validate_staged_event_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L350-L376
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/bounded_meeting_cancellation_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values
---

# Signature

`fn validate_staged_event_property_values( event: &crate::mapi_store::MapiEvent, merged: HashMap<u32, MapiValue>, ) -> Result<()>`

# Calls

- [split_reminder_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values.md)
- [split_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values.md)
- [bounded_meeting_cancellation_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/bounded_meeting_cancellation_from_mapi.md)
- [meeting_response_event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi.md)
- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)

# Called by

- [stage_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values.md)