---
type: Rust Function
title: append_query_named_properties_response
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L730-L748
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_named_properties
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_query_guid
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_query_named_properties_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_named_property_dispatch_response
---

# Signature

`pub(super) async fn append_query_named_properties_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, request: &RopRequest, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [fetch_mapi_named_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_named_properties.md)
- [named_property_query_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_query_guid.md)
- [cache_named_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property.md)
- [rop_query_named_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_query_named_properties_response.md)

# Called by

- [append_named_property_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_named_property_dispatch_response.md)