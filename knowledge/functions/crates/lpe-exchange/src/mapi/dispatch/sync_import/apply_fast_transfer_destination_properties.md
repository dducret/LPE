---
type: Rust Function
title: apply_fast_transfer_destination_properties
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L596-L620
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/apply_pending_associated_message_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response
---

# Signature

`pub(super) fn apply_fast_transfer_destination_properties( session: &mut MapiSession, target_handle: u32, property_values: Vec<(u32, MapiValue)>, ) -> Option<()>`

# Calls

- [apply_pending_associated_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_pending_associated_message_property_values.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [append_fast_transfer_destination_put_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response.md)