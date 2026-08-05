---
type: Rust Function
title: stage_delegate_freebusy_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/property_mutations.rs#L408-L444
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`fn stage_delegate_freebusy_property_values( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, values: Vec<(u32, MapiValue)>, ) -> Result<()>`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [is_outlook_local_freebusy_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)