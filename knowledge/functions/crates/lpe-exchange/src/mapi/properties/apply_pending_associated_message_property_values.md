---
type: Rust Function
title: apply_pending_associated_message_property_values
resource: crates/lpe-exchange/src/mapi/properties.rs#L1363-L1379
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/apply_fast_transfer_destination_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
  - functions/crates/lpe-exchange/src/mapi/properties/apply_mapi_property_values
---

# Signature

`pub(in crate::mapi) fn apply_pending_associated_message_property_values( properties: &mut HashMap<u32, MapiValue>, values: impl IntoIterator<Item = (u32, MapiValue)>, )`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [is_associated_config_read_only_property_tag](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag.md)

# Called by

- [apply_fast_transfer_destination_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/apply_fast_transfer_destination_properties.md)
- [append_synchronization_import_message_change_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
- [apply_mapi_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_mapi_property_values.md)