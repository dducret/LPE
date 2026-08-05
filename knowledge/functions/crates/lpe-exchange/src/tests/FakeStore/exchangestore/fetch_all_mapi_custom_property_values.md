---
type: Rust Method
title: fetch_all_mapi_custom_property_values
resource: crates/lpe-exchange/src/tests/mod.rs#L6901-L6942
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/tests/fake_store_all_custom_property_values_are_scoped_to_one_mapi_object
---

# Signature

`fn fetch_all_mapi_custom_property_values<'a>( &'a self, account_id: Uuid, object_kind: MapiCustomPropertyObjectKind, canonical_id: Uuid, ) -> StoreFuture<'a, Vec<MapiCustomPropertyValue>>`

# Called by

- [copy_all_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request.md)
- [fake_store_all_custom_property_values_are_scoped_to_one_mapi_object](../../../../../../../functions/crates/lpe-exchange/src/tests/fake_store_all_custom_property_values_are_scoped_to_one_mapi_object.md)