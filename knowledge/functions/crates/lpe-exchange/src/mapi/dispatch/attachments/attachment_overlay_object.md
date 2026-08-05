---
type: Rust Function
title: attachment_overlay_object
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1099-L1111
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/apply_event_attachment_overlay_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response
---

# Signature

`pub(super) fn attachment_overlay_object( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, snapshot: &MapiMailStoreSnapshot, ) -> Option<MapiObject>`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [apply_event_attachment_overlay_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/apply_event_attachment_overlay_property.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [append_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response.md)