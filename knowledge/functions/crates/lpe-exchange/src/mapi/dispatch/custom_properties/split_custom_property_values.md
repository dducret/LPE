---
type: Rust Function
title: split_custom_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L3-L9
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/staged_contact_commit_input
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values
---

# Signature

`pub(super) fn split_custom_property_values( values: Vec<(u32, MapiValue)>, ) -> (Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>)`

# Calls

- [is_custom_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/is_custom_property_tag.md)

# Called by

- [staged_contact_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/staged_contact_commit_input.md)
- [split_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_object_property_values.md)
- [validate_pending_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values.md)
- [validate_staged_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values.md)
- [staged_event_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input.md)
- [stage_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values.md)
- [apply_staged_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values.md)