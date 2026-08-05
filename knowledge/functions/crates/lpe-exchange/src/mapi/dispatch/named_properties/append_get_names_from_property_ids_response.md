---
type: Rust Function
title: append_get_names_from_property_ids_response
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L66-L92
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_ids
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_named_properties_by_ids
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_names_from_property_ids_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_named_property_dispatch_response
---

# Signature

`pub(super) async fn append_get_names_from_property_ids_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, request: &RopRequest, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [property_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_ids.md)
- [fetch_mapi_named_properties_by_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_named_properties_by_ids.md)
- [cache_named_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property.md)
- [rop_get_names_from_property_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_names_from_property_ids_response.md)

# Called by

- [append_named_property_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_named_property_dispatch_response.md)