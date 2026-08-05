---
type: Rust Function
title: fast_transfer_named_property_for_message_tag
resource: crates/lpe-exchange/src/mapi/properties/named.rs#L41-L53
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_property_info
---

# Signature

`pub(crate) fn fast_transfer_named_property_for_message_tag( _message_class: &str, property_tag: u32, ) -> Option<MapiNamedProperty>`

# Calls

- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [well_known_named_property_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_for_id.md)

# Called by

- [populate_special_message_named_property_definitions](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/populate_special_message_named_property_definitions.md)
- [write_fast_transfer_property_info](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_property_info.md)