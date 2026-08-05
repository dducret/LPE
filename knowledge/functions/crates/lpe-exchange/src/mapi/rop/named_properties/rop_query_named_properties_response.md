---
type: Rust Function
title: rop_query_named_properties_response
resource: crates/lpe-exchange/src/mapi/rop/named_properties.rs#L32-L47
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/named_properties_for_query
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_query_guid
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_named_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response
---

# Signature

`pub(in crate::mapi) fn rop_query_named_properties_response( request: &RopRequest, session: &MapiSession, ) -> Vec<u8>`

# Calls

- [named_properties_for_query](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/named_properties_for_query.md)
- [named_property_query_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_query_guid.md)
- [write_named_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_named_property.md)

# Called by

- [append_query_named_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_query_named_properties_response.md)