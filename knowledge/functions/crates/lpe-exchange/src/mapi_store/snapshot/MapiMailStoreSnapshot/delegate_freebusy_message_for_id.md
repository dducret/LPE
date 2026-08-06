---
type: Rust Method
title: delegate_freebusy_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1509-L1516
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delegate_freebusy_message_for_open
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`pub(crate) fn delegate_freebusy_message_for_id( &self, item_id: u64, ) -> Option<&MapiDelegateFreeBusyMessage>`

# Called by

- [delegate_freebusy_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delegate_freebusy_message_for_open.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [fast_transfer_manifest_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)