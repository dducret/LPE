---
type: Rust Function
title: apply_canonical_public_folder_item_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L72-L139
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(super) async fn apply_canonical_public_folder_item_property_values<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, item_id: u64, values: Vec<(u32, MapiValue)>, snapshot: &MapiMailStoreSnapshot, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [public_folder_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [apply_mapi_property_values_to_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map.md)
- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)