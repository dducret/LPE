---
type: Rust Function
title: well_known_named_property_for_id
resource: crates/lpe-exchange/src/mapi/properties/named.rs#L28-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/named/explicit_well_known_named_property_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_lid_family_property_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id
  - functions/crates/lpe-exchange/src/mapi/properties/named/fast_transfer_named_property_for_message_tag
  - functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_name_for_id
---

# Signature

`pub(in crate::mapi) fn well_known_named_property_for_id( property_id: u16, ) -> Option<MapiNamedProperty>`

# Calls

- [explicit_well_known_named_property_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/explicit_well_known_named_property_for_id.md)
- [well_known_lid_family_property_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_lid_family_property_for_id.md)

# Called by

- [format_debug_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context.md)
- [well_known_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id.md)
- [fast_transfer_named_property_for_message_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/fast_transfer_named_property_for_message_tag.md)
- [is_reserved_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/is_reserved_named_property_id.md)
- [property_name_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/property_name_for_id.md)