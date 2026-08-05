---
type: Rust Function
title: stage_pending_event_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L170-L251
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_property_is_server_managed
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/apply_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(super) fn stage_pending_event_property_values( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, principal: &AccountPrincipal, values: Vec<(u32, MapiValue)>, ) -> Result<Vec<(usize, u32, u32)>>`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [event_property_is_server_managed](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_property_is_server_managed.md)
- [apply_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/apply_event_property_values.md)
- [validate_pending_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)