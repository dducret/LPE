---
type: Rust Method
title: fetch_or_allocate_mapi_named_property_ids
resource: crates/lpe-exchange/src/tests/mod.rs#L6853-L6911
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/fake_normalize_mapi_named_property
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id
  - functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
---

# Signature

`fn fetch_or_allocate_mapi_named_property_ids<'a>( &'a self, account_id: Uuid, properties: &'a [MapiNamedProperty], create: bool, ) -> StoreFuture<'a, Vec<Option<MapiNamedPropertyMapping>>>`

# Calls

- [fake_normalize_mapi_named_property](../../../../../../../functions/crates/lpe-exchange/src/tests/fake_normalize_mapi_named_property.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [well_known_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id.md)
- [is_reserved_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)