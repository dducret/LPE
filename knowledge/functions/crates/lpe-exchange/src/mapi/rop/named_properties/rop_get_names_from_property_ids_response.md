---
type: Rust Function
title: rop_get_names_from_property_ids_response
resource: crates/lpe-exchange/src/mapi/rop/named_properties.rs#L18-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_ids
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_named_property
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_name_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response
---

# Signature

`pub(in crate::mapi) fn rop_get_names_from_property_ids_response( request: &RopRequest, session: &MapiSession, ) -> Vec<u8>`

# Calls

- [property_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_ids.md)
- [write_named_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_named_property.md)
- [property_name_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_name_for_id.md)

# Called by

- [append_get_names_from_property_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_names_from_property_ids_response.md)