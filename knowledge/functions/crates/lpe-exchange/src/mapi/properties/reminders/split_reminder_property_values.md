---
type: Rust Function
title: split_reminder_property_values
resource: crates/lpe-exchange/src/mapi/properties/reminders.rs#L3-L45
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/tests/reminder_signal_time_wins_independently_of_property_order
---

# Signature

`pub(in crate::mapi) fn split_reminder_property_values( values: Vec<(u32, MapiValue)>, ) -> Result<(HashMap<u32, MapiValue>, Option<bool>, Option<String>)>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [as_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)
- [as_i64](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [validate_pending_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values.md)
- [validate_staged_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values.md)
- [staged_event_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input.md)
- [apply_canonical_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values.md)
- [apply_canonical_task_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values.md)
- [reminder_signal_time_wins_independently_of_property_order](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/reminder_signal_time_wins_independently_of_property_order.md)