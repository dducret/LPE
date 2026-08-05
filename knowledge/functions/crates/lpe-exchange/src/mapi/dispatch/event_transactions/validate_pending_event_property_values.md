---
type: Rust Function
title: validate_pending_event_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L253-L266
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values
---

# Signature

`fn validate_pending_event_property_values( account_id: Uuid, merged: HashMap<u32, MapiValue>, ) -> Result<()>`

# Calls

- [split_reminder_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values.md)
- [split_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values.md)
- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)
- [default_event_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping.md)

# Called by

- [stage_pending_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values.md)