---
type: Rust Function
title: well_known_named_property_id
resource: crates/lpe-exchange/src/mapi/properties/named.rs#L15-L26
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_properties
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_lid_family_property_id
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tag_for_session
  - functions/crates/lpe-exchange/src/mapi/properties/tests/well_known_named_property_mappings_are_bijective
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_id_for_name
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_named_property_ids
---

# Signature

`pub(crate) fn well_known_named_property_id(property: &MapiNamedProperty) -> Option<u16>`

# Calls

- [well_known_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_properties.md)
- [well_known_lid_family_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_lid_family_property_id.md)
- [well_known_named_property_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [normalize_table_property_tag_for_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tag_for_session.md)
- [well_known_named_property_mappings_are_bijective](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/well_known_named_property_mappings_are_bijective.md)
- [property_id_for_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_id_for_name.md)
- [fetch_or_allocate_mapi_named_property_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_named_property_ids.md)