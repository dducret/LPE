---
type: Rust Function
title: append_named_property_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L750-L790
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_named_property_dispatch_response<S>( store: &S, principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, ) -> bool where S: ExchangeStore,`

# Calls

- [append_get_names_from_property_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response.md)
- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [append_query_named_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)