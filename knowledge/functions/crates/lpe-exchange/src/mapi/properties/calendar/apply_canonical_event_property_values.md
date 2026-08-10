---
type: Rust Function
title: apply_canonical_event_property_values
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L1211-L1274
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/bounded_meeting_cancellation_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(in crate::mapi) async fn apply_canonical_event_property_values<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, event_id: u64, values: Vec<(u32, MapiValue)>, snapshot: &MapiMailStoreSnapshot, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [split_reminder_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values.md)
- [bounded_meeting_cancellation_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/bounded_meeting_cancellation_from_mapi.md)
- [meeting_response_event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi.md)
- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)

# Called by

- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)