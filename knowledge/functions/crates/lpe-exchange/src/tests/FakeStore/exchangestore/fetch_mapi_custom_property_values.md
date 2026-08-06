---
type: Rust Method
title: fetch_mapi_custom_property_values
resource: crates/lpe-exchange/src/tests/mod.rs#L6982-L7025
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/fetch_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_custom_property_values_for_request
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_to_copies_custom_values_excluding_tags
  - functions/crates/lpe-exchange/src/tests/fake_store_custom_property_values_survive_restart_style_clone
---

# Signature

`fn fetch_mapi_custom_property_values<'a>( &'a self, account_id: Uuid, object_kind: MapiCustomPropertyObjectKind, canonical_id: Uuid, property_tags: &'a [u32], ) -> StoreFuture<'a, Vec<MapiCustomPropertyValue>>`

# Called by

- [fetch_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/fetch_custom_property_values_for_request.md)
- [copy_custom_property_values_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/copy_custom_property_values_for_request.md)
- [mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay.md)
- [mapi_over_http_microsoft_copy_to_copies_custom_values_excluding_tags](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_to_copies_custom_values_excluding_tags.md)
- [fake_store_custom_property_values_survive_restart_style_clone](../../../../../../../functions/crates/lpe-exchange/src/tests/fake_store_custom_property_values_survive_restart_style_clone.md)