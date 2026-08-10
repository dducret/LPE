---
type: Rust Method
title: upsert_mapi_custom_property_values
resource: crates/lpe-exchange/src/tests/mod.rs#L6962-L6985
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay
  - functions/crates/lpe-exchange/src/tests/fake_store_custom_property_values_survive_restart_style_clone
  - functions/crates/lpe-exchange/src/tests/fake_store_all_custom_property_values_are_scoped_to_one_mapi_object
---

# Signature

`fn upsert_mapi_custom_property_values<'a>( &'a self, account_id: Uuid, object_kind: MapiCustomPropertyObjectKind, canonical_id: Uuid, values: &'a [MapiCustomPropertyValue], ) -> StoreFuture<'a, ()>`

# Called by

- [upsert_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values.md)
- [copy_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_custom_property_values_for_request.md)
- [copy_all_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_all_custom_property_values_for_request.md)
- [mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields.md)
- [mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay.md)
- [fake_store_custom_property_values_survive_restart_style_clone](../../../../../../../functions/crates/lpe-exchange/src/tests/fake_store_custom_property_values_survive_restart_style_clone.md)
- [fake_store_all_custom_property_values_are_scoped_to_one_mapi_object](../../../../../../../functions/crates/lpe-exchange/src/tests/fake_store_all_custom_property_values_are_scoped_to_one_mapi_object.md)