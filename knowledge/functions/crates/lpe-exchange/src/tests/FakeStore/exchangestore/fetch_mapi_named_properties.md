---
type: Rust Method
title: fetch_mapi_named_properties
resource: crates/lpe-exchange/src/tests/mod.rs#L6930-L6955
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_named_property_create_keeps_colliding_property_sets_bijective
---

# Signature

`fn fetch_mapi_named_properties<'a>( &'a self, account_id: Uuid, guid: Option<[u8; 16]>, ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>>`

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [append_query_named_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [mapi_over_http_named_property_create_keeps_colliding_property_sets_bijective](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_named_property_create_keeps_colliding_property_sets_bijective.md)