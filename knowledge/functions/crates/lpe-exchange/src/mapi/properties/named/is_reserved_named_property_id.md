---
type: Rust Function
title: is_reserved_named_property_id
resource: crates/lpe-exchange/src/mapi/properties/named.rs#L55-L57
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/legacy_low_dynamic_property_id_count
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_id_for_name
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_named_property_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_named_property_ids
---

# Signature

`pub(crate) fn is_reserved_named_property_id(property_id: u16) -> bool`

# Calls

- [well_known_named_property_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id.md)

# Called by

- [legacy_low_dynamic_property_id_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/legacy_low_dynamic_property_id_count.md)
- [property_id_for_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_id_for_name.md)
- [allocate_next_mapi_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/allocate_next_mapi_named_property_id.md)
- [fetch_or_allocate_mapi_named_property_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_named_property_ids.md)