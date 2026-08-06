---
type: Rust Function
title: legacy_low_dynamic_property_id_count
resource: crates/lpe-exchange/src/mapi/dispatch/named_properties.rs#L621-L630
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
---

# Signature

`fn legacy_low_dynamic_property_id_count(property_ids: &[u16]) -> usize`

# Calls

- [is_reserved_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id.md)

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)