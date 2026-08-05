---
type: Rust Method
title: property_id_for_name
resource: crates/lpe-exchange/src/mapi/session/named_properties.rs#L5-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/normalize_named_property
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id
  - functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
---

# Signature

`pub(in crate::mapi) fn property_id_for_name( &mut self, property: MapiNamedProperty, create: bool, ) -> Option<u16>`

# Calls

- [normalize_named_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/normalize_named_property.md)
- [try_from](../../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [well_known_named_property_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id.md)
- [is_reserved_named_property_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)