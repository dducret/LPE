---
type: Rust Function
title: upsert_custom_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L32-L65
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_custom_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(super) async fn upsert_custom_property_values<S>( store: &S, principal: &AccountPrincipal, object_kind: MapiCustomPropertyObjectKind, canonical_id: Uuid, values: Vec<(u32, MapiValue)>, ) -> Result<()> where S: ExchangeStore,`

# Calls

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [property_type_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)
- [upsert_mapi_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_custom_property_values.md)

# Called by

- [upsert_custom_property_values_from_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values_from_map.md)
- [apply_staged_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/apply_staged_message_property_values.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)