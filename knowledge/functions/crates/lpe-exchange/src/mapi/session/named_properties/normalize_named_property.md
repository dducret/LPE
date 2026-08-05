---
type: Rust Function
title: normalize_named_property
resource: crates/lpe-exchange/src/mapi/session/named_properties.rs#L144-L153
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_id_for_name
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property
---

# Signature

`pub(in crate::mapi) fn normalize_named_property( mut property: MapiNamedProperty, ) -> MapiNamedProperty`

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [property_id_for_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_id_for_name.md)
- [cache_named_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property.md)