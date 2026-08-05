---
type: Rust Function
title: custom_property_storage_account_id
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L323-L339
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/fetch_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/delete_custom_property_values
---

# Signature

`fn custom_property_storage_account_id( principal: &AccountPrincipal, object: Option<&MapiObject>, snapshot: &MapiMailStoreSnapshot, ) -> Uuid`

# Calls

- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)

# Called by

- [fetch_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/fetch_custom_property_values_for_request.md)
- [copy_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_custom_property_values_for_request.md)
- [copy_all_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request.md)
- [delete_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/delete_custom_property_values.md)