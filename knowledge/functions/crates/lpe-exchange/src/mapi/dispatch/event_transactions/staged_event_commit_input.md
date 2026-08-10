---
type: Rust Function
title: staged_event_commit_input
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L524-L632
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/bounded_meeting_cancellation_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_calendar_pending_recipients
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
---

# Signature

`pub(super) fn staged_event_commit_input( principal: &AccountPrincipal, event: &crate::mapi_store::MapiEvent, transaction: &MapiEventTransaction, reminder: Option<&lpe_storage::ClientReminder>, force_save: bool, ) -> Result<Option<MapiEventCommitInput>>`

# Calls

- [split_reminder_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values.md)
- [split_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values.md)
- [bounded_meeting_cancellation_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/bounded_meeting_cancellation_from_mapi.md)
- [meeting_response_event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi.md)
- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [apply_calendar_pending_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_calendar_pending_recipients.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [property_type_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)
- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)

# Called by

- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)