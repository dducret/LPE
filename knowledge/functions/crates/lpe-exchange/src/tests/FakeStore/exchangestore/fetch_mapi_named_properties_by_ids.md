---
type: Rust Method
title: fetch_mapi_named_properties_by_ids
resource: crates/lpe-exchange/src/tests/mod.rs#L6912-L6932
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response
---

# Signature

`fn fetch_mapi_named_properties_by_ids<'a>( &'a self, account_id: Uuid, property_ids: &'a [u16], ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_get_names_from_property_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response.md)