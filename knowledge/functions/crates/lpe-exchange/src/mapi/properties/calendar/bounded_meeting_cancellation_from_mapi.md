---
type: Rust Function
title: bounded_meeting_cancellation_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L1154-L1191
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values
---

# Signature

`pub(in crate::mapi) fn bounded_meeting_cancellation_from_mapi( properties: &HashMap<u32, MapiValue>, ) -> Result<bool>`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)

# Called by

- [validate_staged_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values.md)
- [staged_event_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input.md)
- [apply_canonical_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values.md)