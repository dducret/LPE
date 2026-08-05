---
type: Rust Function
title: cache_named_property_mapping_and_return_property_id
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L56-L64
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_named_property_mapping_keeps_registered_database_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_returns_registered_well_known_property_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_returns_registered_contact_source_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_keeps_registered_reserved_range_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/store_named_property_mapping_rejects_session_collision
---

# Signature

`pub(super) fn cache_named_property_mapping_and_return_property_id( session: &mut MapiSession, property_id: u16, property: MapiNamedProperty, ) -> u16`

# Calls

- [cache_named_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/cache_named_property.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [calendar_named_property_mapping_keeps_registered_database_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_named_property_mapping_keeps_registered_database_ids.md)
- [get_property_ids_from_names_returns_registered_well_known_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_returns_registered_well_known_property_id.md)
- [get_property_ids_from_names_returns_registered_contact_source_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_returns_registered_contact_source_id.md)
- [get_property_ids_from_names_keeps_registered_reserved_range_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_keeps_registered_reserved_range_id.md)
- [store_named_property_mapping_rejects_session_collision](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/store_named_property_mapping_rejects_session_collision.md)